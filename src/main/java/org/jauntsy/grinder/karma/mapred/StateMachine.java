package org.jauntsy.grinder.karma.mapred;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.karma.operations.reduce.Sum;
import org.jauntsy.grinder.karma.testing.InMemoryReducerState;
import org.jauntsy.grinder.panama.UniversalComparator;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import storm.trident.state.JSONNonTransactionalSerializer;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static org.jauntsy.nice.Nice.list;
import static junit.framework.Assert.assertEquals;

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
public class StateMachine {

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

    public StateMachine(String viewName, Fields keyFields, Fields valueFields, Reducer reducer, ReducerState state) {
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

    public UpdateResult update(List key, List docId, List newDocValue) {
        if (DEBUG)
            System.out.println(viewName + ".sm.update key: " + key + ", doc: " + docId + ", value: " + newDocValue);
//        List _oldTotal = null, _newTotal = null;
        try {
            String keyAsString = keyToString(key);
            String docIdAsString = keyToString(docId);

            List<Column> columns = new ArrayList<Column>();
            long startingHash = crc(docIdAsString);

            for (int depth = 0; depth < TREE_DEPTH; depth++) {
                columns.add(new Column(startingHash, depth));
            }

            Map<Column, Map<String, List>> writeQueue = new HashMap<Column, Map<String, List>>(); // column -> key -> tuple

            List<String> columnValuesAsJson = getAll(keyAsString, columns);
            List<Map<String,List>> columnValues = new ArrayList<Map<String,List>>();
            for (int i = 0; i < TREE_DEPTH; i++) {
                String json = columnValuesAsJson.get(i);
                Map<String, List> decodedSuperColumns = new TreeMap<String, List>();
                if (json != null) {
                    JSONObject jo = (JSONObject) JSONValue.parse(json);
                    for (String joKey : (Set<String>)jo.keySet()) {
                        List list = (List) jo.get(joKey);
                        List joValue = list == null ? null : new ArrayList(list);
                        decodedSuperColumns.put(joKey, joValue);
                    }
                }
                columnValues.add(decodedSuperColumns);
            }

            List _oldTotal = columnValues.get(0).get("_t");
            List _newTotal = update(writeQueue, key, keyAsString, docIdAsString, newDocValue, columns, columnValues, 0);

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
            if (root != null && !isListEquivalent(_oldTotal, _newTotal)) {
                ret = new UpdateResult(
                        true,
                        _newTotal == null ? null : new SimpleTuple(valueFields, _newTotal),
//                        new SimpleTuple(valueFields, _newTotal),
                        _oldTotal == null ? null : new SimpleTuple(valueFields, _oldTotal)
                );
            } else {
                Map<String, List> rootValues = columnValues.get(0);
                ret = new UpdateResult(
                        false,
                        new SimpleTuple(
                                valueFields,
                                rootValues == null ? nullValues() : (List) rootValues.get("_t")
                        ),
                        null
                );
            }
//        List _oldTotal = null;
            return ret;
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private List update(Map<Column, Map<String, List>> writeQueue, List rowKey, String rowKeyAsString, String docIdAsString, List newDocValue, List<Column> columns, List<Map<String, List>> columnValues, int depth) {
        Column column = columns.get(depth);
        Map<String, List> values = columnValues.get(depth);

        boolean recalc = false;

        if (values.containsKey(docIdAsString)) {
            List currentValue = values.get(docIdAsString);
            if (isListEquivalent(currentValue, newDocValue)) {
                return values.get("_t");
            } else {
                values.put(docIdAsString, newDocValue);
                recalc = true;
            }
        } else {
            String superColName = superColName(crc(docIdAsString), depth);
            if (values.containsKey(superColName)) {
                List oldValue = values.get(superColName);
                List newValue = update(writeQueue, rowKey, rowKeyAsString, docIdAsString, newDocValue, columns, columnValues, depth + 1);
                if (isListEquivalent(oldValue, newValue)) {
                    return values.get("_t");
                } else {
                    values.put(superColName, newValue);
                    recalc = true;
                }
            } else if (values.size() <= 16) {
                values.put(docIdAsString, newDocValue);
                writeQueue.put(column, values);
                recalc = true;
            } else {
                Set<String> toRemove = new HashSet<String>();
                Map<String,List> toAdd = new HashMap<String,List>();
                values.put(docIdAsString, newDocValue);
                for (Map.Entry<String,List> e : values.entrySet()) {
                    String key = e.getKey();
                    List value = e.getValue();
                    if (key.startsWith("[")) {
                        String nextNodeId = superColName(crc(key), depth);
                        if (superColName.equals(nextNodeId)) {
                            List update = update(writeQueue, rowKey, rowKeyAsString, key, value, columns, columnValues, depth + 1);
                            toAdd.put(
                                    nextNodeId,
                                    update
                            );
                            toRemove.add(key);
                        }
                    }
                }
                for (String it : toRemove)
                    values.remove(it);
                values.putAll(toAdd);
                recalc = true;
            }
        }

        // *** recalc total
        if (recalc) {
            writeQueue.put(column, values);
            List oldTotal = values.get("_t");
            List total = null;
            for (Map.Entry<String,List> e : values.entrySet()) {
                String key = e.getKey();
                if (!key.startsWith("_")) {
                    List value = e.getValue();
                    if (value != null) {
                        total = total == null ? value : reducer.reduce(new SimpleTuple(keyFields, rowKey), new SimpleTuple(valueFields, total), new SimpleTuple(valueFields, value));
                    }
                }
            }
            values.put("_t", total);
        }

        return values.get("_t");

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

    public static void main(String[] args) {
        InMemoryReducerState state = new InMemoryReducerState(false);
        state.prepare(null, null, "test");
        StateMachine sm = new StateMachine("test", new Fields(), new Fields("total"), new Sum(), state);
//        sm.update(list(), list("update",0), list(1));
//        sm.update(list(), list("update",1), list(2));
//        UpdateResult update = sm.update(list(), list("update", 2), list(3));
//        System.out.println("update.result = " + update.result);

        for (int i = 0; i < 25; i++) {
            UpdateResult update = sm.update(list(), list("update", i), list(1));
            System.out.println("update.result = >>>>>>>>>>>> " + update.result);
        }

        for (int i = 0; i < 25; i++) {
            UpdateResult update = sm.update(list(), list("update", i), null);
            System.out.println("update.result = >>>>>>>>>>>> " + update.result);
        }

//        System.out.println("update.result = " + update.result);
    }

}
