package com.exacttarget.spike.experiments;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * User: ebishop
 * Date: 1/7/13
 * Time: 12:10 PM
 */
public class WhenDoEmitsFromBoltsActuallyHappen {
    public static void main(String[] args) {
        LocalCluster cluster = new LocalCluster();

        FeederSpout spout = new FeederSpout(new Fields("name", "age"));
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", spout, 1);
        builder.setBolt("bolt1", new Bolt1(), 1).shuffleGrouping("spout");
        builder.setBolt("bolt2", new Bolt2(), 1).shuffleGrouping("bolt1");
        cluster.submitTopology("topo", new Config(), builder.createTopology());


        spout.feed(new Values("joe", 27));

        Utils.sleep(100000);
    }

    public static class Bolt1 extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            for (int i = 0; i < 10; i++) {
                System.out.println("bolt1.execute Emitting: " + i);
                this.collector.emit(tuple, new Values(i));
                Utils.sleep(10000);
            }
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }
    }

    public static class Bolt2 extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            System.out.println("bolt2.execute got: " + tuple.getValues());
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

    }

}
