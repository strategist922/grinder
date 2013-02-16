package org.jauntsy.grinder.karma.mapred;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.panama.UniversalComparator;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import storm.trident.state.JSONNonTransactionalSerializer;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * User: ebishop
 * Date: 1/7/13
 * Time: 3:34 PM
 * <p/>
 * TODO: This needs to either cache writes until all downstream processing has finished or
 * always send the running total upstream on update, even if nothing has changed.
 * <p/>
 * This is to protect against the case of local state writing to disk before the downstream consumers have
 * consumed the events. It is possible for local state to get written, downstream to crash/exit before the
 * update is sent. On resend from the spout, the update will find nothing to do but still needs to send the
 * total upstream. The alternative is a long and cumbersome chain of commit() events flowing back through the
 * bolts. Ugly.
 * <p/>
 * One option is if reducer writes to 2 streams.... default and changes, any write produces something on default
 * but only actual value changes get sent to 'changes'
 */
public class StateMachineOld {

    private boolean DEBUG = false;

    private static final Map<Integer, Integer> TREES = new HashMap<Integer, Integer>() {{
        put(2, 63);
        put(4, 32);
        put(8, 21);
        put(16, 16); // *** 16x16x4 @ 192_000 == red:71 and map:2152 op/s 30x
        put(32, 13);
        put(64, 11);
        put(128, 9);
        put(256, 8);
    }};

//    private static int[] ROW_ID_SHIFT_TABLE = {64, 64, 64, 64, 48, 48, 48, 48, 32, 32, 32, 32, 16, 16, 16, 16 };
//
//    public static long CALC_ROW_ID(long hashOfDocId, int depth) { // depth 0..15
//        int shift = ROW_ID_SHIFT_TABLE[depth] * 4;
//        if (shift >= 64) throw new IllegalArgumentException("hash: " + hashOfDocId + ", depth: " + depth);
//        return shift >= 64 ? 0L : hashOfDocId >> shift;
//    }
//
//    public static long CALC_COL_ID(long hashOfDocId, int depth) { // depth 0..15
//        int shift = (16 - depth) * 4;
//        return shift >= 64 ? 0L : hashOfDocId >> shift;
//    }

    public static long superColHash(long hash, int depth) {
        int shift = 15 - depth;
        for (int i = 0; i < shift; i++) {
            hash = hash / 16;
        }
        return hash;
    }

    public static String superColName(long hash, int depth) {
        return (depth + 1) + ":" + superColHash(hash, depth);
    }

    public static String colName(long hash, int depth) {
        return depth + ":" + superColHash(hash, depth - 1);
    }

    public static String rowName(long hash, int depth) {
        depth = ((int)(depth)/4) * 4 - 1;
        return (depth + 1) + ":" + superColHash(hash, depth);
    }

    public static void drill(long v)  {
        System.out.println("\n### " + v);
        for (int depth = 15; depth >= 0; depth--) {
            System.out.println(rowName(v, depth) + " => " + colName(v, depth) + " => { " + superColName(v, depth) + " = value, ..., ... }");
        }
    }

    public static void main(String[] args) {
        drill(Long.MIN_VALUE);
        drill(Long.MIN_VALUE >> 1);
        drill(Long.MIN_VALUE >> 2);
        drill(Long.MIN_VALUE >> 3);
        drill(Long.MIN_VALUE >> 4);
//32.times { v -> drill((long)v) }
        drill(-1L);
        drill(0L);
        drill(1L);
        drill(Long.MAX_VALUE);
        drill(Long.MAX_VALUE >> 1);
        drill(Long.MAX_VALUE >> 2);
        drill(Long.MAX_VALUE >> 3);
        drill(Long.MAX_VALUE >> 4);
    }


//    private static final Map<Integer,List<Integer>> partition

    //    private static final int VALUES_PER_LEAF = 256;
    private static final int VALUES_PER_LEAF = 16;
    private static final int TREE_DEPTH = TREES.get(VALUES_PER_LEAF);

    private final String viewName;
    private final Fields keyFields;
    private final Fields valueFields;
    private final Reducer reducer;
    private final ReducerState state;

    private transient Benchmark updateBench;
    private transient Benchmark writeBench;
    private transient Benchmark processBench;
    private transient Benchmark readBench;

    public StateMachineOld(String viewName, Fields keyFields, Fields valueFields, Reducer reducer, ReducerState state) {
        this.viewName = viewName;
        this.keyFields = keyFields;
        this.valueFields = valueFields;
        this.reducer = reducer;
        this.state = new CachedReducerState(state, 10000);
//        this.keyToColumns = new HashMap<String,Map<String,String>>();
        this.updateBench = new Benchmark("sm.update");
        this.writeBench = new Benchmark("sm.write");
        this.processBench = new Benchmark("sm.process");
        this.readBench = new Benchmark("sm.read");
    }

    public static long crc(String s) {
        long base = Crc64.getCrc(toBytes(s));
        return ((base / 2) + (Long.MAX_VALUE / 2) + 1);
    }

    public static long crc(List key) {
        return crc(keyToString(key));
    }

    public UpdateResult update(List key, List docId, List newLeafValue) {
        if (DEBUG)
            System.out.println(viewName + ".sm.update key: " + key + ", doc: " + docId + ", value: " + newLeafValue);
        List _oldTotal = null, _newTotal = null;
        List _superColumnsAsJsonDebug = null;
        try {
            Benchmark.Start updateStart = updateBench.start();
            String keyAsString = keyToString(key);
            String docIdAsString = keyToString(docId);

//        List<String> columnNames = new ArrayList<String>();
            List<Column> columns = new ArrayList<Column>();
            long startingHash = crc(docIdAsString);

            for (int depth = TREE_DEPTH - 1; depth >= 0; depth--) {
                columns.add(new Column(startingHash, depth));
            }

            Map<Column, Map<String, List>> writeQueue = new HashMap<Column, Map<String, List>>(); // column -> key -> tuple

            Benchmark.Start readStart = readBench.start();
            List<String> superColumnsAsJsonByColumn = getAll(keyAsString, columns);
            _superColumnsAsJsonDebug = superColumnsAsJsonByColumn;
            if (DEBUG)
                readStart.end();

            Benchmark.Start processStart = processBench.start();
            String superColumnName = docIdAsString;
            List newNodeValue = newLeafValue;
//        List _oldTotal = null;
            _oldTotal = null;
            boolean[] hasNewTotal = new boolean[16];
            for (int i = 0; i < TREE_DEPTH; i++) {
                int depth = 15 - i; // leaf nodes first
                String json = superColumnsAsJsonByColumn.get(i);
                Map<String, List> decodedSuperColumns = new TreeMap<String, List>();
                if (json != null) {
                    JSONObject jo = (JSONObject) JSONValue.parse(json);
                    for (String joKey : (Set<String>)jo.keySet()) {
                        List list = (List) jo.get(joKey);
                        List joValue = list == null ? null : new ArrayList(list);
                        decodedSuperColumns.put(joKey, joValue);
                    }
                }
                List oldSuperColumnValue = decodedSuperColumns.get(superColumnName);
                if (newNodeValue == null) {
                    if (oldSuperColumnValue == null) {
                        break;
                    } else {
                        decodedSuperColumns.remove(superColumnName);
                    }
                } else {
                    if (isListEquivalent(oldSuperColumnValue, newNodeValue)) {
                        break;
                    } else {
                        decodedSuperColumns.put(superColumnName, newNodeValue);
                    }
                }

                // *** a value in decoded was modified so save the results
                Column column = columns.get(i);
                writeQueue.put(column, decodedSuperColumns);

                // *** recalc
                SimpleTuple keyAsTuple = new SimpleTuple(keyFields, key);
                SimpleTuple total = null;
                for (Map.Entry<String, List> e : decodedSuperColumns.entrySet()) {
                    if (!e.getKey().startsWith("_")) {
                        if (total == null) {
                            total = new SimpleTuple(valueFields, e.getValue());
                        } else {
                            List newTotal = reducer.reduce(keyAsTuple, total, new SimpleTuple(valueFields, e.getValue()));
                            total = new SimpleTuple(valueFields, newTotal);
                        }
                    }
                }
                newNodeValue = total == null ? null : total.getValues();
                _newTotal = newNodeValue;
                _oldTotal = decodedSuperColumns.get("_t");
                if (_oldTotal == null || !isListEquivalent(_oldTotal, _newTotal)) {
//                    System.out.println(viewName + " key: " + key + ", _newTotal = " + _newTotal);
                    decodedSuperColumns.put("_t", _newTotal);
                    hasNewTotal[i] = true;
                    superColumnName = superColName(startingHash, depth - 1);
                } else {
                    break;
                }
            }
            if (DEBUG)
                processStart.end();

            _newTotal = newNodeValue;

            // *** write the pending chanages; TODO: don't abort if no change since we are now using multiple columns
            if (writeQueue.size() > 0) {
                    Benchmark.Start writeStart = writeBench.start();
                    Map<String, Map<String, String>> batchesByRowPrefix = new TreeMap<String, Map<String, String>>();
                    for (Column column : writeQueue.keySet()) {
                        Map<String, List> columnValue = writeQueue.get(column);
                        String columnValueAsJson = JSONValue.toJSONString(columnValue);
                        String rowPrefix = column.rowName;
                        Map<String, String> batch = batchesByRowPrefix.get(rowPrefix);
                        if (batch == null) {
                            batch = new TreeMap<String, String>();
                            batchesByRowPrefix.put(rowPrefix, batch);
                        }
                        batch.put(column.colName, columnValueAsJson);
                    }
                    for (String rowPrefix : batchesByRowPrefix.keySet()) {
                        String rowName = rowPrefix + ":" + keyAsString;
                        state.putAll(rowName, batchesByRowPrefix.get(rowPrefix));
                    }
                    if (DEBUG)
                        writeStart.end();
            }

            Map<String, List> root = writeQueue.get(new Column(startingHash, 0));
            UpdateResult ret = null;
            if (root != null) {
                List rootTotal = root.get("_t");
                ret = new UpdateResult(
                        hasNewTotal[TREE_DEPTH - 1],
                        new SimpleTuple(valueFields, rootTotal),
                        _oldTotal == null ? null : new SimpleTuple(valueFields, _oldTotal)
                );
            } else {
                String rootAsJson = superColumnsAsJsonByColumn.get(superColumnsAsJsonByColumn.size() - 1);
                ret = new UpdateResult(false, new SimpleTuple(valueFields, rootAsJson == null ? nullValues() : (List) (((JSONObject) JSONValue.parse(rootAsJson)).get("_t"))), null);
            }
            if (DEBUG) {
                updateStart.end();
            }
            return ret;
        } catch (RuntimeException ex) {
            System.out.println("Error processing stuff");
            System.out.println("StateMachine.update => newValue: " + newLeafValue);
            System.out.println("_oldTotal = " + _oldTotal);
            System.out.println("_newTotal = " + _newTotal);
            System.out.println("isListEquivalent(_oldTotal, _newValue) = " + isListEquivalent(_oldTotal, _newTotal));
            System.out.println("_columnsAsJsonDebug = " + _superColumnsAsJsonDebug);
            throw ex;
        }
    }

    private List<String> getAll(String keyAsString, List<Column> columns) {
        Map<String, List<String>> batchRequest = new HashMap<String, List<String>>();
        Map<String, List<String>> columnNamesByRowName = new HashMap<String, List<String>>();
        for (Column column : columns) {
            String rowName = column.rowName + ":" + keyAsString;
            List<String> row = columnNamesByRowName.get(rowName);
            if (row == null) {
                row = new ArrayList<String>();
                columnNamesByRowName.put(rowName, row);
                batchRequest.put(rowName, row);
            }
            row.add(column.colName);
        }

        Map<String, String> results = new HashMap<String, String>();
        Map<String, Map<String, String>> batchResult = state.getBatch(batchRequest);
//        System.out.println("StateMachine.getAll result: " + batchResult);
        for (Map<String, String> values : batchResult.values()) {
            results.putAll(values);
        }
        List<String> ret = new ArrayList<String>();
        for (Column column : columns)
            ret.add(results.get(column.colName));
        return ret;
    }

    private List nullValues() {
        List ret = new ArrayList(valueFields.size());
        for (int i = 0; i < valueFields.size(); i++)
            ret.add(null);
        return ret;
    }

    public static String keyToString(List key) {
        return JSONValue.toJSONString(key);
    }

    private static String toString(Object o) {
        try {
            return new String(new JSONNonTransactionalSerializer().serialize(o), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] toBytes(String s) {
        try {
            return s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isListEquivalent(List a, List b) {
        if (a == null) {
            return b == null;
        } else if (b == null) {
            return false;
        } else {
            if (a.size() != b.size())
                return false;
            for (int i = 0; i < a.size(); i++) {
                Object _a = a.get(i);
                Object _b = b.get(i);
                if (!isObjectEquivalent(_a, _b))
                    return false;
            }
        }
        return true;
    }

    private static boolean isObjectEquivalent(Object a, Object b) {
        if (a == null) {
            return b == null;
        } else if (b == null) {
            return false;
        } else {
            if (a instanceof Number) {
                if (!(b instanceof Number))
                    return false;
                else
                    return 0 == UniversalComparator.INSTANCE.compareNumbers((Number) a, (Number) b);
            } else if (a instanceof String) {
                return a.equals(b);
            } else if (a instanceof List) {
                return isListEquivalent((List) a, (List) b);
            } else {
                return a.equals(b);
            }
        }
    }

    public static class UpdateResult {
        public final boolean changed;
        public final Tuple result;
        public final Tuple prevResults;

        public UpdateResult(boolean changed, Tuple result, Tuple prevResult) {
            this.changed = changed;
            this.result = result;
            this.prevResults = prevResult;
        }
    }


    public class Column {

        private final long docIdHash;
        private final int depth;

        public final String rowName;
        public final String colName;

        public Column(long docIdHash, int depth) {
            this.docIdHash = docIdHash;
            this.depth = depth;
            this.rowName = rowName(docIdHash, depth);
            this.colName = colName(docIdHash, depth);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Column)) return false;

            Column column = (Column) o;

            if (depth != column.depth) return false;
            if (docIdHash != column.docIdHash) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (docIdHash ^ (docIdHash >>> 32));
            result = 31 * result + depth;
            return result;
        }

        public String toString() {
            return String.format("Column(%s,%s)", rowName, colName);
        }

    }

    public static class Kv2<A,V> extends HashMap<A,V> {

    }

    public static class Kv3<A,B,V> {

        Map<A,Kv2<B,V>> impl = new HashMap<A,Kv2<B,V>>();

        public Kv2<B,V> get(A a) {
            return impl.get(a);
        }

        public V get(A a, B b) {
            Kv2<B,V> kv2 = get(a);
            return kv2 == null ? null : kv2.get(b);
        }

        public void put(A a, B b, V v) {
            Kv2<B, V> kv2 = impl.get(a);
            if (kv2 == null) {
                kv2 = new Kv2<B, V>();
                impl.put(a, kv2);
            }
            kv2.put(b, v);
        }

    }

    public static class SuperColumn<A,B,C> {
        public final A row;
        public final B column;
        public final C property;

        public SuperColumn(A row, B column, C property) {
            this.row = row;
            this.column = column;
            this.property = property;
        }
    }

}
