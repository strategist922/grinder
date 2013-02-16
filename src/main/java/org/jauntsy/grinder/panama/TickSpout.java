package org.jauntsy.grinder.panama;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * User: ebishop
 * Date: 12/14/12
 * Time: 1:45 PM
 */
public class TickSpout extends BaseRichSpout {

    private long frequencyMs;

    private transient SpoutOutputCollector collector;

    public TickSpout(long frequencyMs) {
        this.frequencyMs = frequencyMs;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("tick", new Fields());
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        collector.emit("tick", new Values());
        Utils.sleep(frequencyMs);
    }

}
