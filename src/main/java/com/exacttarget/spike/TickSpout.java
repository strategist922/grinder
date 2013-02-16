package com.exacttarget.spike;

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
 * Date: 11/27/12
 * Time: 5:41 PM
 */
public class TickSpout extends BaseRichSpout {

    private long interval;

    private SpoutOutputCollector collector;

    public TickSpout() {
        this(1000L);
    }

    public TickSpout(long interval) {
        this.interval = interval;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ts"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(interval);
        collector.emit(new Values(System.currentTimeMillis()));
    }

}
