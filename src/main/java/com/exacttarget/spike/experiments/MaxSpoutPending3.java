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
 * Who blocks where when a bolt exceeds maxSpoutPending
 *
 * Spout "A" emits to bolt "A"
 * Spout "B" emits to bolt "B"
 *
 * Both emit to "C"
 * "C" only acks "A"
 *
 *
 * if you set maxspout on the spout this settings wins
 * THEN the config for deployTopology
 * on the bolt, this property seems to be ignored....
 *
 */
public class MaxSpoutPending3 {
    public static void main(String[] args) {
        LocalCluster cluster = new LocalCluster();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new SpoutA());//.setMaxSpoutPending(10);

        BoltDeclarer boltA = builder.setBolt("boltA", new TestBolt("a"));
        boltA.shuffleGrouping("spout").setMaxSpoutPending(20);

        BoltDeclarer boltB = builder.setBolt("boltB", new TestBolt("b"));
        boltB.shuffleGrouping("boltA").setMaxSpoutPending(30);

        Config config = new Config();
//        config.setMaxSpoutPending(40);
        cluster.submitTopology("test", config, builder.createTopology());

        Utils.sleep(100000);

        cluster.shutdown();
    }

    public static class SpoutA extends BaseRichSpout {

        private transient int next;
        private transient SpoutOutputCollector collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("heartbeat"));
        }

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
            this.collector = collector;
            this.next = 0;
        }

        @Override
        public void ack(Object msgId) {
            System.out.println("ack: " + msgId);
            super.ack(msgId);    //To change body of overridden methods use File | Settings | File Templates.
        }

        @Override
        public void nextTuple() {
            Values tuple = new Values(next);
            collector.emit(tuple, next);
            next++;
            Utils.sleep(100);
        }

    }

    public static class TestBolt extends BaseRichBolt {

        private final String name;

        private OutputCollector collector;

        public TestBolt(String name) {
            this.name = name;
        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            boolean log = true;//"a".equals(name);
            if (log) System.out.println(name + " IN => got tuple: " + tuple.getValues() + " from " + tuple.getSourceComponent());
            collector.emit(tuple, tuple.getValues());
            if ("a".equals(name))
                collector.ack(tuple);
            if (log) System.out.println(name + " OUT");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("foo"));
        }
    }

}
