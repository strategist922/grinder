package org.jauntsy.grinder.karma.mapred;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.*;

import static junit.framework.Assert.assertEquals;

/**
 * User: ebishop
 * Date: 1/7/13
 * Time: 1:04 PM
 */
public class ReducerBolt extends BaseRichBolt {

    private static final boolean DEBUG = false;

    private final String viewName;
    private final Fields keyFields;
    private final Fields valueFields;
    private final Fields intermediateFields;
    private final Reducer reducer;
    private final Set<String> mapperIds;
    private final Fields formatterOutputValueFields;
    private final Fields formatterOutputFields;
    private final Formatter formatter;
    private final ReducerState state;

    private transient OutputCollector collector;
    private StateMachine stateMachine;
    private String topologyName;

    public ReducerBolt(String viewName, Fields keyFields, Fields valueFields, Reducer reducer, Set<String> mapperIds, Fields formatterOutputValueFields, Formatter formatter, ReducerState state) {
        this.viewName = viewName;
        this.keyFields = keyFields;
        this.valueFields = valueFields;
        List<String> f = new ArrayList<String>();
        f.addAll(keyFields.toList());
        f.addAll(valueFields.toList());
        this.intermediateFields = new Fields(f);
        this.reducer = reducer;
        this.mapperIds = mapperIds;

        this.formatterOutputValueFields = formatterOutputValueFields;
        this.formatter = formatter;

        if (formatter != null) {
            f = new ArrayList();
            f.addAll(keyFields.toList());
            f.addAll(formatterOutputValueFields.toList());
            this.formatterOutputFields = new Fields(f);
        } else {
            this.formatterOutputFields = null;
        }

        this.state = state;
        if (state == null) throw new IllegalArgumentException();
    }

    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.topologyName = (String)config.get("topology.name");
        this.collector = collector;
        this.state.prepare(config, context, topologyName + "_" + viewName + "_red");
        this.stateMachine = new StateMachine(context.getThisComponentId(), keyFields, valueFields, reducer, state);
        if (this.reducer != null)
            this.reducer.prepare(config, context);
        if (this.formatter != null)
            this.formatter.prepare(config, context);
    }

    @Override
    public void execute(final Tuple tuple) {
        try {
            if (DEBUG) {
                System.out.println("!!!!!! " + viewName + " reducer: " + tuple.getValues());
            }

            int srcTaskId = tuple.getInteger(0);
            int txId = tuple.getInteger(1);
            List key = (List) tuple.getValue(2);
            String src = tuple.getString(3);
            List id = (List) tuple.getValue(4);
            List newValue = (List) tuple.getValue(5);

            List docId = new ArrayList();
            docId.add(src);
            docId.addAll(id);

            StateMachine.UpdateResult update = stateMachine.update(key, docId, newValue);
            if (formatter != null) {
                List newFormattedValue = buildFormattedResult(key, update.result);
                collector.emit(tuple, newFormattedValue);

                if (update.changed) {
                    List oldFormattedValue = buildFormattedResult(key, update.prevResults);
                    if (!StateMachine.isListEquivalent(newFormattedValue, oldFormattedValue)) {
                        collector.emit("changes", tuple, newFormattedValue);
                    }
                }
            } else {
                Tuple runningTotal = update.result;
                List out = new ArrayList();
                out.addAll(key);
                List<Object> values = runningTotal == null ? null : runningTotal.getValues();

                if (values == null) {// || values.contains(null))) { // *** TODO: make a decision on this
                    // *** don't emit partial sets, any null makes all null
                    for (int i = 0; i < valueFields.size(); i++)
                        out.add(null);
                    collector.emit(tuple, out);
                    // *** only emit a change if the previous value was "non-null"
                    if (update.changed) {
                        // *** TODO: allow nulls? Dunno, dunno, dunno.
                        if (update.prevResults != null) {// && !update.prevResults.getValues().contains(null))
                            collector.emit("changes", tuple, out);
                        }
                    }
                } else {
                    out.addAll(values);
                    collector.emit(tuple, out);
                    // *** only emit a change if the value is different from the last value
                    if (update.changed) {
                        collector.emit("changes", tuple, out);
                    }
                }
            }
            if (!MapperBolt.NEW_STYLE) {
                collector.emitDirect(srcTaskId, tuple.getSourceComponent(), tuple, tuple.getValues());
            }
            collector.ack(tuple);
        } catch (Exception ex) {
            System.out.println(viewName + ": Error processing " + tuple.getValues());
            throw new RuntimeException(ex);
        }
    }

    private List buildFormattedResult(List key, Tuple reduceResult) {
//        System.out.println("ReducerBolt.buildFormattedResult key: " + key + ", value: " + reduceResult);
        List formattedValue = new ArrayList();
        formattedValue.addAll(key);
        boolean newTotalIsNull = reduceResult == null || reduceResult.getValues() == null || reduceResult.getValues().contains(null);
        if (newTotalIsNull) {
            for (int i = 0; i < formatterOutputValueFields.size(); i++)
                formattedValue.add(null);
        } else {
            formattedValue.addAll(formatter.merge(reduceResult));
        }
        return formattedValue;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (formatter == null) {
            declarer.declareStream("default", false, intermediateFields);
            declarer.declareStream("changes", false, intermediateFields);
        } else {
            declarer.declareStream("default", false, formatterOutputFields);
            declarer.declareStream("changes", false, formatterOutputFields);
        }
        for (String mapperId : mapperIds) {
            declarer.declareStream(mapperId, true, new Fields("taskId", "txId", "key", "source", "id", "value"));
        }
    }

}

/*

NOTES

key comes in

def get = new Get(toBytes(key))
def hash = hash64(key)
get.addColumn("nodes", "16:" + hash))
get.addColumn("nodes", "15:" + (hash / 16^1))
get.addColumn("nodes", "14:" + (hash / 16^2))
get.addColumn("nodes", "13:" + (hash / 16^3))
get.addColumn("nodes", "12:" + (hash / 16^4))
...
get.addColumn("nodes", "01:" + (hash / 16^16)) // *** always 0
get.addColumn("nodes", "00")

we read 16 columns from row 'key'
 each column is 16 values [['a'],[1]... ]

starting at column 16 (which has keys, not an array)
sum up columns, check total against next level up (ie. current hash / 16)
if same, repeat with next level (sum up check etc.)

at level 01:? we have a total which is compared to 00
each stage MAY have to be changed, ie a value substituted

writes are accumulated and then
written
new key+value is sent downstream
incoming tuple is sent back upstream to be confirmed by sender
tuple is acked


The state machine should maintain a cache of nodes in memory.
When reading from the ReducerState, use the cache...

*/