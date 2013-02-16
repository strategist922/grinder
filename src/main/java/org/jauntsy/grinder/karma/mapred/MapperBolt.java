package org.jauntsy.grinder.karma.mapred;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.jauntsy.grinder.karma.Emitter;
import org.jauntsy.grinder.panama.UniversalComparator;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import java.util.*;

import static org.jauntsy.nice.Nice.L;
import static org.jauntsy.nice.Nice.M;

/**
 * User: ebishop
 * Date: 1/7/13
 * Time: 1:05 PM
 * <p/>
 * TODO: pending txId, this is kind of un-stormy and on a fail, dunno if it will ever clear
 * we need to pick up the expiration time for the tuples and also expire ours within some multiple
 * of the spout expiration
 */
public class MapperBolt extends BaseRichBolt {

    private static final boolean DEBUG = false;
    public static final boolean NEW_STYLE = true;

    private final String srcComponentId;

    /*
    Used to namespace the document id of the incoming document ie. from src:userSpout, srcName:'a', id 7 => id:['a',7]
     */
    private final String srcName;

    private final Fields docIdFields;
    private final Fields docValueFields;
    private final String viewName;
    private final Fields keyFields;
    private final Fields valueFields;
    private final Mapper mapper;
    private final Reducer reducer;
    private final ReducerState state;

    private transient int taskId;
    private transient OutputCollector collector;

    //    private TreeMap<List, Tuple> currentSavedValueTuple;
//    private TreeMap<List, Tuple> currentCachedValueTuple;
    private TreeMap<List, PrevCurr> prevCurrCachedValueTuple;

    private Map<List, Tuple> prevMappings;
    private Map<List, Tuple> nextMappings;
    private Map<List, Tuple> _mappings;
    private Emitter emitter;
    private int nextTxId;
    private TreeMap<Integer, Integer> pendingCounts;
    private TreeMap<Integer, List> pendingDocPaths;
    //    private Map<Integer, Tuple> pendingValueTuples;
    private Map<Integer, PrevCurr> pendingPrevCurrValueTuples;
    private Map<Integer, Tuple> pendingAnchorTuples;
    private Map<List, Integer> pendingTransactionsPerDocPath;
    private String topologyName;

    public MapperBolt(String srcComponentId, String srcName, Fields docIdFields, Fields docValueFields, String viewName, Fields keyFields, Fields valueFields, Mapper mapper, Reducer reducer, ReducerState state) {
        if (DEBUG) System.out.println("MapperBolt.MapperBolt srcComponentId: " + srcComponentId);
        this.srcComponentId = srcComponentId;
        this.srcName = srcName;
        this.docIdFields = docIdFields;
        this.docValueFields = docValueFields;
        this.viewName = viewName;
        this.keyFields = keyFields;
        this.valueFields = valueFields;
        this.mapper = mapper;
        this.reducer = reducer;
        if (docIdFields == null) throw new IllegalArgumentException();
        if (docValueFields == null) throw new IllegalArgumentException();

        this.state = state;
        if (state == null)
            throw new IllegalArgumentException();
    }

    @Override
    public void prepare(Map stormConfig, TopologyContext context, OutputCollector collector) {

        this.topologyName = (String) stormConfig.get("topology.name");

        if (DEBUG) System.out.println("MapperBolt.prepare stormConfig: " + stormConfig + ", context: " + context);
        this.taskId = context.getThisTaskId();
        this.collector = collector;

        this.mapper.prepare(stormConfig, context);
        this.reducer.prepare(stormConfig, context);

//        this.currentSavedValueTuple = new TreeMap<List,Tuple>(UniversalComparator.LIST_COMPARATOR);
        this.prevCurrCachedValueTuple = new TreeMap<List, PrevCurr>(UniversalComparator.LIST_COMPARATOR);

        this.prevMappings = new TreeMap<List, Tuple>(UniversalComparator.LIST_COMPARATOR);
        this.nextMappings = new TreeMap<List, Tuple>(UniversalComparator.LIST_COMPARATOR);
        this._mappings = null;
        final int emitParamCount = keyFields.size() + valueFields.size();
        this.emitter = new Emitter() {
            @Override
            public void emit(Object... values) {
                if (values.length < emitParamCount)
                    throw new IllegalArgumentException("Expected " + emitParamCount + " value in emit()");
                List key = new ArrayList();
                for (int i = 0; i < keyFields.size(); i++) {
                    key.add(values[i]);
                }
                List value = new ArrayList();
                for (int i = 0; i < valueFields.size(); i++)
                    value.add(values[keyFields.size() + i]);
                Tuple current = _mappings.get(key);
                if (current == null) {
                    _mappings.put(key, new SimpleTuple(valueFields, value));
                } else {
                    _mappings.put(
                            key,
                            new SimpleTuple(
                                    valueFields,
                                    reducer.reduce(
                                            new SimpleTuple(keyFields, key),    // *** key
                                            current,                            // *** running total
                                            new SimpleTuple(valueFields, value)  // *** value to be folded into running total
                                    )
                            )
                    );
                }
            }

        };
        this.nextTxId = taskId * 10000;
        this.pendingCounts = new TreeMap<Integer, Integer>();
        this.pendingDocPaths = new TreeMap<Integer, List>();
        this.pendingPrevCurrValueTuples = new HashMap<Integer, PrevCurr>();
        this.pendingAnchorTuples = new HashMap<Integer, Tuple>();
        this.pendingTransactionsPerDocPath = new TreeMap<List, Integer>(UniversalComparator.LIST_COMPARATOR);

        this.state.prepare(stormConfig, context, topologyName + "_" + viewName + "_map");
    }

    private Map<List, Tuple> map(Tuple doc, Map<List, Tuple> dest) {
        this._mappings = dest;
        this._mappings.clear();
        if (isFullyDefined(doc)) {
            mapper.map(doc, emitter);
        }
        return this._mappings;
    }

    /*
    Returns true if all value fields are non-null
    Returns false if any value field is null (docId fields can be null)
     */
    private boolean isFullyDefined(Tuple t) {
        return isFullyDefined(docIdFields, t);
    }

    private boolean isFullyDefined(Fields docIdFields, Tuple t) {
        return t != null && isFullyDefined(docIdFields, t.getFields(), t.getValues());
    }

    private boolean isFullyDefined(Fields docIdFields, Fields fields, List values) {
        if (values == null || values.size() == 0) return false;
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value == null && !docIdFields.contains(fields.get(i)))
                return false;
        }
        return true;
    }

    private PrevCurr getPrevCurrValueTuple(List docPath) {
        if (prevCurrCachedValueTuple.containsKey(docPath)) {
            return prevCurrCachedValueTuple.get(docPath);
        } else {
            PrevCurr ret = getPrevCurrSavedValueTuple(docPath);
            prevCurrCachedValueTuple.put(docPath, ret);
            return ret;
        }
    }

    //    private Tuple getCurrentValueTuple(List docPath) {
//        if (currentCachedValueTuple.containsKey(docPath)) {
//            return currentCachedValueTuple.get(docPath);
//        } else {
//            Tuple ret = getCurrentSavedValueTuple(docPath);
//            currentCachedValueTuple.put(docPath, ret);
//            return ret;
//        }
//    }
//
    private PrevCurr getPrevCurrSavedValueTuple(List docPath) {
        Map<String, String> all = state.getAll(JSONValue.toJSONString(docPath), L("prev", "curr"));
        String prevJson = all.get("prev");
        String currentJson = all.get("curr");
        Tuple prev = prevJson == null ? null : new SimpleTuple(docValueFields, (JSONArray) JSONValue.parse(prevJson));
        Tuple current = currentJson == null ? null : new SimpleTuple(docValueFields, (JSONArray) JSONValue.parse(currentJson));
        return new PrevCurr(prev, current);
    }

    //    private Tuple getCurrentSavedValueTuple(List docPath) {
//        String current = state.getAll(JSONValue.toJSONString(docPath), L("current")).get("current");
//        if (current == null)
//            return null;
//        return new SimpleTuple(docValueFields, (JSONArray) JSONValue.parse(current));
//    }
//
    @Override
    public void execute(Tuple tupleIn) {
        try {
            if ("default".equals(tupleIn.getSourceStreamId())) {
                if (DEBUG)
                    System.out.println(viewName + ".map.default tupleIn from " + tupleIn.getSourceComponent() + ":" + srcName + " = " + tupleIn.getValues());

                int txId = nextTxId++;
                List<Object> _docId = tupleIn.select(docIdFields);
                List<Object> docValue = tupleIn.select(docValueFields);


                List docPath = new ArrayList();
                docPath.add(srcName);
                docPath.addAll(_docId);

                // *** TODO: when a mapper throws an exception, we can treat the value as a CLEAR
            /*
            If we store every version of every object, we can flag a version as invalid and 'ignore' it while
            also allowing the version to be inspected. 0
             */
                Tuple nextValueTuple = (docValue != null && isFullyDefined(docIdFields, docValueFields, docValue)) ? new SimpleTuple(docValueFields, docValue) : null;

                PrevCurr lastPrevCurr = getPrevCurrValueTuple(docPath);
                Tuple lastPrevValue = lastPrevCurr.prev;
                Tuple lastCurrValue = lastPrevCurr.curr;

                Tuple currValueTuple;
                if (StateMachine.isListEquivalent(lastCurrValue == null ? null : lastCurrValue.getValues(), nextValueTuple == null ? null : nextValueTuple.getValues())) {
                    currValueTuple = lastPrevValue;
                } else {
                    currValueTuple = lastCurrValue;
                }

                // *** map the previous value
                map(currValueTuple, prevMappings);

                // *** map the next value
                map(nextValueTuple, nextMappings);

                // *** send the mapped values to the reducers
                int updatesSent = 0;
                for (Map.Entry<List, Tuple> e : nextMappings.entrySet()) {
                    List key = e.getKey();
                    List<Object> value = e.getValue().getValues();
                    Values tuple = new Values(taskId, txId, key, srcName, _docId, value);
                    collector.emit(tupleIn, tuple);
                    updatesSent++;
                    prevMappings.remove(key); // *** if a key was sent with a new value we don't have to clear it in the next step
                }

                // *** send the deleted keys to the reducers
                int deletesSent = 0;
                for (Map.Entry<List, Tuple> e : prevMappings.entrySet()) {
                    List key = e.getKey();
                    collector.emit(tupleIn, new Values(taskId, txId, key, srcName, _docId, null));
                    deletesSent++;
                }

                if (NEW_STYLE) {
                    collector.ack(tupleIn);
                } else {

                    int txPendingCount = updatesSent + deletesSent;
                    if (txPendingCount > 0) {
//                    currentCachedValueTuple.put(docPath, nextValueTuple);
                        PrevCurr nextPrevCurrValueTuple = new PrevCurr(currValueTuple, nextValueTuple);
                        prevCurrCachedValueTuple.put(docPath, nextPrevCurrValueTuple);

                        pendingCounts.put(txId, txPendingCount);
                        pendingDocPaths.put(txId, docPath);
                        pendingPrevCurrValueTuples.put(txId, nextPrevCurrValueTuple);
                        pendingAnchorTuples.put(txId, tupleIn);

                        Integer pendingTransactions = pendingTransactionsPerDocPath.get(docPath);
                        pendingTransactionsPerDocPath.put(docPath, pendingTransactions == null ? 1 : pendingTransactions + 1);
                    } else {
                        collector.ack(tupleIn);
                    }
                }


            } else {
                if (NEW_STYLE)
                    throw new IllegalStateException("New Style Violation");
                if (!NEW_STYLE) {
                    int txId = tupleIn.getInteger(1);
                    Integer pendingCount = pendingCounts.get(txId);
                    pendingCounts.put(txId, pendingCount - 1);
                    while (!pendingCounts.isEmpty() && pendingCounts.firstEntry().getValue().equals(0)) {
                        int completedTxId = pendingCounts.firstKey();
                        pendingCounts.remove(completedTxId);
                        PrevCurr completedPrevCurrValueTuple = pendingPrevCurrValueTuples.remove(completedTxId);

                        List completedDocPath = pendingDocPaths.remove(completedTxId);

                        int remainingTransactionsForDocPath = pendingTransactionsPerDocPath.get(completedDocPath);
                        if (remainingTransactionsForDocPath == 1) {
                            prevCurrCachedValueTuple.remove(completedDocPath);
                            pendingTransactionsPerDocPath.remove(completedDocPath);
                        } else {
                            pendingTransactionsPerDocPath.put(completedDocPath, remainingTransactionsForDocPath - 1);
                        }

                        saveCurrentValueTuple(completedDocPath, completedPrevCurrValueTuple);
                        Tuple completedTuple = pendingAnchorTuples.remove(completedTxId);
                        collector.ack(completedTuple);
                    }
                }
                collector.ack(tupleIn);
            }
        } catch (RuntimeException ex) {
            System.out.println("docIdFields = " + docIdFields);
            System.out.println("docValueFields = " + docValueFields);
            throw new RuntimeException(viewName + ": Exception processing tuple; fields: " + tupleIn.getFields() + ", values: " + tupleIn.getValues(), ex);
        }
    }

    private void saveCurrentValueTuple(List docPath, PrevCurr prevCurrValueTuples) {
        String key = JSONValue.toJSONString(docPath);
        Tuple prev = prevCurrValueTuples.prev;
        Tuple current = prevCurrValueTuples.curr;
        String prevValue = prev == null ? null : JSONValue.toJSONString(prev.getValues());
        String currValue = current == null ? null : JSONValue.toJSONString(current.getValues());
        state.putAll(key, M("prev", prevValue));
        state.putAll(key, M("curr", currValue));
    }
//
//    private void saveCurrentValueTuple(List docPath, Tuple valueTuple) {
//        String key = JSONValue.toJSONString(docPath);
//        String value = valueTuple == null ? null : JSONValue.toJSONString(valueTuple.getValues());
//        state.putAll(key, M("current", value));
//    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "txId", "key", "source", "id", "value"));
    }

    private static class PrevCurr {
        public final Tuple prev;
        public final Tuple curr;

        private PrevCurr(Tuple prev, Tuple curr) {
            this.prev = prev;
            this.curr = curr;
        }
    }

}
