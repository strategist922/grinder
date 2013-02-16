package org.jauntsy.grinder.karma.testing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.karma.mapred.SimpleTuple;
import org.jauntsy.grinder.karma.mapred.nextgen.BaseRichBatchBolt;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 1/8/13
 * Time: 9:29 AM
 */
public class PrintBolt extends BaseRichBatchBolt {

    private final String label;
    private final long frequency;
    private final boolean showStats;

    private transient OutputCollector collector;
    private long lastReport;
    private long calls;
    private long updatesSinceLastReport;

    public PrintBolt(String label) {
        this(label, 0, false);
    }

    public PrintBolt(String label, int frequency, boolean showStats) {
        super(1000, frequency);
        this.label = label;
        this.frequency = frequency;
        this.calls = 0;
        this.updatesSinceLastReport = 0;
        this.showStats = showStats;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        super.prepare(map, topologyContext, outputCollector);
    }

    @Override
    public final void executeBatch(List<Tuple> tuples) {
        Iterator<Tuple> itor = tuples.iterator();
        while(itor.hasNext()) {
            Tuple tuple = itor.next();
            boolean isLastTupleOfBatch = !itor.hasNext();
            calls++;
            updatesSinceLastReport++;
            long now = System.currentTimeMillis();
            long timeSinceLastReport = now - lastReport;
            if (isLastTupleOfBatch) {
                Long qps = timeSinceLastReport <= 0 ? null : 1000L * updatesSinceLastReport / timeSinceLastReport;
                if (showStats)
                    System.out.println(label + " calls: " + calls + ", op/s: " + qps + ", value: " + getMessage(tuple) + " (" + calls + ")");
                else
                    System.out.println(label + ": " + getMessage(tuple));
                lastReport = now;
                updatesSinceLastReport = 0;
            }
            collector.ack(tuple);
        }
    }

    public String getMessage(Tuple tuple) {
        return String.valueOf(new SimpleTuple(tuple.getFields(), tuple.getValues()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
