package com.exacttarget.spike.experiments;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * User: ebishop
 * Date: 12/28/12
 * Time: 10:01 AM
 *
 * Test what happens when max spout pending gets exceeded...
 * Who blocks
 *
 * Setup: 2 spouts A and B, 1 bolt... tuples from spout A are not acked
 * Spouts produce 20 tuples at a time
 *
 * Result: spout "A" nuxtTuple stops getting calls after the first pass of 20 tuples
 *         spout "B" continues emitting tuples forever
 *
 * The spout never blocks when outputing tuples, its Storm simply does not call nextTuple
 *
 */
public class MaxSpoutPending2 {
    public static void main(String[] args) {
        LocalCluster cluster = new LocalCluster();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("a", new SpoutA("a")).setMaxSpoutPending(10);
        builder.setSpout("b", new SpoutA("b")).setMaxSpoutPending(10);

        BoltDeclarer bolt = builder.setBolt("bolt", new TestBolt());
        bolt.shuffleGrouping("a", "a");
        bolt.shuffleGrouping("b", "b");

        cluster.submitTopology("test", new Config(), builder.createTopology());

        Utils.sleep(100000);

        cluster.shutdown();
    }

    public static class SpoutA extends BaseRichSpout {

        private final String stream;

        private transient int next;
        private transient SpoutOutputCollector collector;

        public SpoutA(String stream) {
            this.stream = stream;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream(stream, new Fields("heartbeat"));
        }

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
            this.collector = collector;
            this.next = 0;
        }

        @Override
        public void ack(Object msgId) {
            System.out.println(stream + " ack: " + msgId);
            super.ack(msgId);    //To change body of overridden methods use File | Settings | File Templates.
        }

        @Override
        public void nextTuple() {
            boolean log = "a".equals(stream);
            if (log) System.out.println("nextTuple(" + stream + ") - IN");
            for (int i = 0; i < 20; i++) {
            Values tuple = new Values(stream + ":" + next);
            if (log) System.out.println("emitting: " + tuple);
            collector.emit(stream, tuple, next);
            next++;
            }
            Utils.sleep(100);
            if (log) System.out.println("nextTuple(" + stream + ") - OUT");
        }

    }

    public static class TestBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            System.out.println("got tuple: " + tuple.getValues());
            if (tuple.getSourceStreamId().equals("b")) {
                collector.ack(tuple);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }

}
