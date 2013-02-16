package org.jauntsy.grinder.karma.mapred.nextgen;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.karma.mapred.Reducer;import org.jauntsy.grinder.karma.mapred.SimpleTuple;
import org.jauntsy.grinder.karma.mapred.StateMachine;
import org.jauntsy.grinder.panama.UniversalListComparator;
import org.json.simple.JSONValue;

import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.*;

import static org.jauntsy.nice.Nice.L;

/**
 * User: ebishop
 * Date: 1/22/13
 * Time: 12:03 PM
 */
public class ReduceSnapshot {

    private static final int LEAF_WIDTH = 16;
    private static final int TREE_HEIGHT = 16;
    private static final int MAX_DEPTH = TREE_HEIGHT - 1;

    private Reducer reducer;

//    Ma Reducer ySnapshot> table;

    public List set(List key, List docId, List value) {
        return null;
    }

    public static class KeySnapshot {
        Object id;
        Map<Object,KeySnapshot> children;
        List _total;
    }

    public static class Path {
        List id;
    }

    public static class Tree {

        private final Fields idFields;
        private final Fields valueFields;
        private final RawReducer reducer;
        private final Map<List,KeyTree> keys;
        private final Set<List> dirty;
        private Map<List, Map<List, Map<Object, List>>> ret;

        public Tree(final Fields idFields, final Fields valueFields, final Reducer reducer) {
            this.idFields = idFields;
            this.valueFields = valueFields;
            this.reducer = new RawReducer(idFields, valueFields, reducer);
            this.keys = new TreeMap<List,KeyTree>(UniversalListComparator.INSTANCE);
            this.dirty = new TreeSet<List>(UniversalListComparator.INSTANCE);
        }

        public void put(List key, List docId, List value) {
            KeyTree keyTree = this.keys.get(key);
            if (keyTree == null) {
                keyTree = new KeyTree(this, key);
                this.keys.put(key, keyTree);
            }
            if (keyTree.put(docId, value)) {
                dirty.add(key);
            }
        }

        public List getValue(List key) {
            KeyTree keyTree = keys.get(key);
            return keyTree == null ? null : keyTree._total;
        }

        public Stats calcStats() {
            Stats stats = new Stats();
            calcStats(stats);
            return stats;
        }

        private void calcStats(Stats stats) {
            for (KeyTree keyTree : keys.values()) {
                keyTree.calcStats(stats);
            }
        }

        public void writeTo(PrintStream out) {
            out.println("{" + (dirty.size() > 0 ? " *" : ""));
            for (List key : keys.keySet()) {
                KeyTree keyTree = keys.get(key);
                out.println("  " + key + ": {" + (keyTree.isDirty() ? " *" : ""));
                keyTree.write(out);
                out.println("  }");
            }
            out.println("}");
        }

        public void markClean() {
            for (List dirtyChild : dirty) {
                keys.get(dirtyChild).markClean();
            }
            dirty.clear();
        }

        public Map<String, Map<String, String>> getDirtyRows() {
            Map<String, Map<String, String>> ret = new HashMap<String, Map<String, String>>();
            for (List dirtyChild : dirty) {
                keys.get(dirtyChild).getDirtyColumns(ret);
            }
            return ret;
        }
    }

    public static class KeyTree {

        private final Tree tree;
        private final List key;
        private final Node root;

        private List _total;

        public KeyTree(final Tree tree, final List key) {
            this.tree = tree;
            this.key = key;
            this.root = new Node();
        }

        public boolean put(List docId, List value) {
            boolean ret = root.put(key, docId, value, tree.reducer);
            _total = root.total;
            return ret;
        }

        public void calcStats(Stats stats) {
            stats.keys++;
            root.calcStats(stats, 0);
        }

        public void write(PrintStream out) {
            root.write(out, 0);
        }

        public void markClean() {
            root.markClean();
        }

        public boolean isDirty() {
            return root.dirty;
        }

        public void getDirtyColumns(Map<String, Map<String, String>> ret) {
            root.getDirtyColumns(ret, key, StateMachine.crc(key), 0);
        }
    }

    public static class Node {

        Map<Long,Node> children;
        Map<List,List> values;
        TreeSet<List> dirtyValues;
        List total;
        boolean dirty = false;

        public Node() {
            this.children = null;
            this.values = new TreeMap<List,List>(UniversalListComparator.INSTANCE);
            this.dirtyValues = new TreeSet<List>(UniversalListComparator.INSTANCE);
        }

        public boolean put(List key, List docId, List value, RawReducer reducer) {
            return put(key, docId, value, reducer, 0);
        }

        public boolean put(List key, List docId, List value, RawReducer reducer, int depth) {
            if (depth == MAX_DEPTH || values.containsKey(docId) || (children == null && values.size() < LEAF_WIDTH)) {
                if (values.containsKey(docId)) {
                    List oldValue = values.get(docId);
                    if (0 != UniversalListComparator.INSTANCE.compare(value, oldValue)) {
                        values.put(docId, value);
                        dirtyValues.add(docId);
                        dirty = true;
                    } else {
                    }
                } else {
                    values.put(docId, value);
                    dirtyValues.add(docId);
                    dirty = true;
                }
            } else {
                if (children == null)
                    children = new HashMap<Long, Node>();
                values.put(docId, value);
                Map<Long,List> mappedIds = new HashMap<Long,List>();
                Set<List> toRemove = new TreeSet<List>(UniversalListComparator.INSTANCE);
                for (Map.Entry<List,List> entry : values.entrySet()) {
                    List childDocId = entry.getKey();
                    List childValue = entry.getValue();
                    long childNodeId = getHashForDepth(childDocId, depth);
                    if (children.containsKey(childNodeId)) {
                        dirty = dirty | children.get(childNodeId).put(key, childDocId, childValue, reducer, depth + 1);
                        toRemove.add(childDocId);
                    } else if (mappedIds.containsKey(childNodeId)) {
                        List otherDocId = mappedIds.remove(childNodeId);
                        List otherValue = values.get(otherDocId);
                        Node newNode = new Node();
                        children.put(childNodeId, newNode);
                        newNode.put(key, otherDocId, otherValue, reducer, depth + 1);
                        newNode.put(key, childDocId, childValue, reducer, depth + 1);
                        toRemove.add(otherDocId);
                        toRemove.add(childDocId);
                        dirtyValues.add(otherDocId);
                        dirtyValues.add(childDocId);
                        dirty = true;
                    } else {
                        mappedIds.put(childNodeId, childDocId);
                    }
                }
                for (List valueDocId : toRemove) {
                    values.remove(valueDocId);
                }
                if (values.containsKey(docId)) {
                    dirtyValues.add(docId);
                    dirty = true;
                }
            }
            updateTotal(reducer, key);
            return dirty;
        }

        private void updateTotal(RawReducer reducer, List key) {
            List total = null;
            if (children != null) {
                for (Node child : children.values()) {
                    if (reducer.isValid(child.total))
                        total = total == null ? child.total : reducer.reduce(key, total, child.total);
                }
            }
            for (List value : values.values()) {
                if (reducer.isValid(value))
                    total = total == null ? value : reducer.reduce(key, total, value);
            }
            this.total = total;
        }

        public void calcStats(Stats stats, int depth) {
            stats.nodes++;
            stats.values += values.size();
            stats.depth = Math.max(stats.depth, depth + 1);
//            for (int i = 0; i < depth; i++)
//                System.out.print("  ");
//            System.out.println(values.size());
            if (children != null) {
                for (Node child : children.values())
                    child.calcStats(stats, depth + 1);
            }
        }

        public void write(PrintStream out, int depth) {
            for (List key : values.keySet()) {
                out.println(pad(depth) + key + ": " + values.get(key) + (dirtyValues.contains(key) ? " *" : ""));
            }
            if (children != null) {
                for (Long childNodeKey : children.keySet()) {
                    Node node = children.get(childNodeKey);
                    out.println(pad(depth) + childNodeKey + ": {" + (node.dirty ? " *" : ""));
                    node.write(out, depth + 1);
                    out.println(pad(depth) + "}");
                }
            }
            out.println(pad(depth) + "_total: " + total);
        }

        public void markClean() {
            if (children != null) {
                for (Node child : children.values()) {
                    child.markClean();
                }
            }
            this.dirtyValues.clear();
            dirty = false;
        }

        public void getDirtyColumns(Map<String, Map<String, String>> ret, List key, long hash, int depth) {
            String rowName = StateMachine.rowName(hash, depth);
            String colName = StateMachine.colName(hash, depth);
            String superColName = StateMachine.superColName(hash, depth);
            Map<String,String> row = ret.get(rowName);
            if (row == null) {
                row = new HashMap<String,String>();
                ret.put(rowName, row);
            }
            Map m = new HashMap();
            for (List valueKey : values.keySet()) {
                m.put(JSONValue.toJSONString(valueKey), JSONValue.toJSONString(values.get(valueKey)));
            }
            if (children != null) {
                for (Long childKey : children.keySet()) {
                    m.put("" + (depth + 1) + ":" + childKey, JSONValue.toJSONString(children.get(childKey).total));
                }
            }
            row.put(colName, JSONValue.toJSONString(m));
        }
    }

    private boolean isValid(Fields expected, List value) {
        if (value == null) return false;
        if (value.size() < expected.size()) return false;
        for (Object o : value)
            if (o == null)
                return false;
        return true;
    }

    private static Charset UTF8 = Charset.forName("UTF-8");

    private static long getHashForDepth(List id, int depth) {
        return StateMachine.superColHash(StateMachine.crc(id), depth);
    }

    public static class RawReducer {

        private final Fields keyFields;
        private final Fields valueFields;
        private final SimpleTuple _key;
        private final SimpleTuple _a;
        private final SimpleTuple _b;
        private final Reducer impl;

        public RawReducer(Fields keyFields, Fields valueFields, Reducer impl) {
            this.keyFields = keyFields;
            this.valueFields = valueFields;
            _key = new SimpleTuple(keyFields, null);
            _a = new SimpleTuple(valueFields, null);
            _b = new SimpleTuple(valueFields, null);
            this.impl = impl;
        }

        public List reduce(List key, List a, List b) {
            _key.setValues(key);
            _a.setValues(a);
            _b.setValues(b);
            return new ArrayList(impl.reduce(_key, _a, _b));
        }

        public boolean isValid(List value) {
            if (value == null)
                return false;
            if (value.size() != valueFields.size())
                return false;
            for (Object o : value)
                if (o == null)
                    return false;
            return true;
        }
    }

    public static class Stats {
        public int keys = 0;
        public int nodes = 0;
        public int values = 0;
        public int depth = 0;

        public String toString() {
            int spaceRatio = nodes > 0 ? values / nodes : 0;
            int calcRatio = nodes / ((1 + depth) * LEAF_WIDTH);
            return String.format("stats(:keys %d :nodes %d :values %d :depth %d :space-ratio %d :calc-ratio %d)", keys, nodes, values, depth, spaceRatio, calcRatio);
        }
    }


    static public String pad(int depth) {
        switch(depth) {
            case 0: return "    ";
            case 1: return "      ";
            case 2: return "        ";
            case 3: return "          ";
            case 4: return "            ";
            default: throw new IllegalArgumentException("" + depth);
        }
    }

    public static void main(String[] args) {
        Tree tree = new Tree(new Fields("account"), new Fields("balance"), new Reducer() {
            @Override
            public List reduce(Tuple key, Tuple a, Tuple b) {
                return L(a.getFloat(0) + b.getFloat(0));
            }
        });
        tree.put(L("a"), L("deposits", 0), L(100.0));
        tree.put(L("a"), L("deposits", 0), L(100.0));
        tree.put(L("a"), L("deposits", 1), L(100.0));
        tree.put(L("a"), L("withdrawals", 0), L(-100.0));
        System.out.println("a._total = " + tree.getValue(L("a")));
        tree.writeTo(System.out);
        tree.put(L("a"), L("withdrawals", 0), null);
        System.out.println("a._total = " + tree.getValue(L("a")));
        tree.writeTo(System.out);

        for (int i = 0; i < 40; i++) {
            tree.put(L("a"), L("deposits", i), L(1.0));
        }
        tree.writeTo(System.out);
        tree.markClean();
        tree.put(L("a"), L("deposits", 40), L(1.0));
        Map<String, Map<String, String>> dirtyData = tree.getDirtyRows();
        System.out.println("dirtyData = " + dirtyData);
    }

}
