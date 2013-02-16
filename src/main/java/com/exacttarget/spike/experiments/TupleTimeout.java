package com.exacttarget.spike.experiments;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 1/22/13
 * Time: 11:06 AM
 *
 * What happens when a downstream tuple timeout occurs.
 *
 * What happens if a bolt is just going slow and continues processing a tuple which has already "failed" at the spout.
 * What indication is there of 2nd delivery?
 *
 * Basically, nothing downstream knows anything about nothing. Ie. if a tuple does a timeout, only the spout know
 * However, no ACK for the tuple will ever happen. Ie.
 *
 * send msg -> a -> b
 *
 * if it expires the spout will get a fail(someid), a will continue to process and b will get the output of a BUT
 * when ack is called, nothing is received at the spout, no ack, no fail
 *
 */
public class TupleTimeout {
    public static void main(String[] args) {
        Logger.getRootLogger().setLevel(Level.WARN);
        LocalCluster cluster = new LocalCluster();
        TopologyBuilder builder = new TopologyBuilder();
        TestSpout spout = new TestSpout(new Fields("msg"));
        builder.setSpout("spout", spout);
        builder.setBolt("bolt_a", new BoltA(new Fields("msg"))).globalGrouping("spout");
        builder.setBolt("bolt_b", new BoltB()).globalGrouping("bolt_a");
        Config config = new Config();
        config.setMessageTimeoutSecs(5);
        cluster.submitTopology("test", config, builder.createTopology());
        spout.feed(new Values("msgA"));
        spout.feed(new Values("msgB"));
        spout.feed(new Values("msgC"));
        Utils.sleep(100000);
    }

    public static class TestSpout extends FeederSpout {

        public TestSpout(Fields outFields) {
            super(outFields);
        }

        @Override
        public void ack(Object msgId) {
            System.out.println("TupleTimeout$TestSpout.ack: " + msgId);
            super.ack(msgId);    //To change body of overridden methods use File | Settings | File Templates.
        }

        @Override
        public void fail(Object msgId) {
            System.out.println("TupleTimeout$TestSpout.fail: " + msgId);
            super.fail(msgId);    //To change body of overridden methods use File | Settings | File Templates.
        }

    }

    public static class BoltA extends BaseRichBolt {

        private final Fields fields;

        private transient OutputCollector collector;

        public BoltA(Fields fields) {
            this.fields = fields;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        boolean slept = false;

        @Override
        public void execute(Tuple input) {
//            System.out.println("TupleTimeout$BoltA.execute: " + input);
            System.out.println("A: got " + input.getString(0));
            if (!slept && "msgB".equals(input.getString(0))) {
                System.out.println("Sleeping so a timeout occurs");
                Utils.sleep(10000);
                Values toSend = new Values(input.getString(0) + " (post sleep)");
                System.out.println("A: sending " + toSend);
                this.collector.emit(input, toSend);
                slept = true;
            } else {
                System.out.println("A: sending " + input.getString(0));
                this.collector.emit(input, input.getValues());
            }
            this.collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(fields);
        }

    }

    public static class BoltB extends BaseRichBolt {

        private transient OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
//            System.out.println("TupleTimeout$BoltB.execute: " + input);
            System.out.println("B: got " + input.getString(0));
            this.collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

    }

}
