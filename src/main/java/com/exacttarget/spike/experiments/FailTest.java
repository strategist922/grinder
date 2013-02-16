package com.exacttarget.spike.experiments;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
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

import java.util.Map;

/**
 * User: ebishop
 * Date: 2/14/13
 * Time: 4:10 PM
 *
 * What does fail do?
 *
 */
public class FailTest {

    public static void main(String[] args) {

        Logger.getRootLogger().setLevel(Level.WARN);

        LocalCluster cluster = new LocalCluster();

        TopologyBuilder builder = new TopologyBuilder();

        IRichSpout spout = new BaseRichSpout() {

            public SpoutOutputCollector collector;

            private transient int id;

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("id"));
            }

            @Override
            public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
                this.collector = collector;
                this.id = 1;
            }

            @Override
            public void nextTuple() {
                collector.emit(new Values(id), id);
                id++;
                Utils.sleep(100);
            }

            @Override
            public void ack(Object msgId) {
                System.out.println("FailTest.ack " + msgId);
                super.ack(msgId);
            }

            @Override
            public void fail(Object msgId) {
                System.out.println("FailTest.fail " + msgId);
                super.fail(msgId);
            }
        };
        builder.setSpout("spout", spout, 1);
        builder.setBolt("bolt", new TestBolt(), 1).globalGrouping("spout");

        cluster.submitTopology("test", new Config(), builder.createTopology());

        Utils.sleep(100000);
    }

    public static class TestBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            System.out.println("FailTest$TestBolt.prepare");
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            int value = input.getInteger(0);
            if (isEven(value)) {
                collector.fail(input);
            } else {
                collector.ack(input);
            }
        }

        private boolean isEven(int value) {
            return (value % 2 == 0);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
