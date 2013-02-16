package org.jauntsy.grinder.karma.mapred.nextgen;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * User: ebishop
 * Date: 2/11/13
 * Time: 10:50 AM
 */
public abstract class BasicKvBolt implements IRichKvBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        this.execute(input, collector);
        collector.ack(input);
    }

    public abstract void execute(Tuple input, OutputCollector collector);

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
