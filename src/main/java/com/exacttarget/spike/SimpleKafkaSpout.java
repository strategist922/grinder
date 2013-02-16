package com.exacttarget.spike;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.util.Map;
import java.util.TreeSet;

/**
 * User: ebishop
 * Date: 11/29/12
 * Time: 11:25 AM
 */
public class SimpleKafkaSpout extends BaseRichSpout {

    private static int MAX_PENDING = 1000;

    private String host;
    private int port;
    private int soTimeout;
    private int bufferSize;

    private String topic;
    private int partition;

    private SimpleConsumer consumer;
    private SpoutOutputCollector collector;
    private long offset;

    private TreeSet pending;
    private long lastFetch;

    public SimpleKafkaSpout(String host, int port, int soTimeout, int bufferSize, String topic, int partition) {
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("str"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.consumer = new SimpleConsumer(host, port, soTimeout, bufferSize);
        this.collector = collector;
        this.offset = KafkaUtil.getOffsetBeforeLatestTime(consumer, topic, partition);
        this.pending = new TreeSet();
    }

    @Override
    public void nextTuple() {
        try {
            if (pending.size() < MAX_PENDING) {
                FetchRequest fetchRequest = new FetchRequest(topic, partition, offset, 64 * 1024);
                ByteBufferMessageSet fetch = consumer.fetch(fetchRequest);
                for (MessageAndOffset mno : fetch) {
                    Long id = mno.offset();
                    pending.add(id);
                    collector.emit(new Values(new String(KafkaUtil.copyBytes(mno.message()))), id);
                    offset = mno.offset();
                }
                lastFetch = System.currentTimeMillis();
            } else {
                System.out.println("Backing off: pending.size = " + pending.size());
                Utils.sleep(10);
            }
        } catch(Exception ex) {
            ex.printStackTrace();
            Utils.sleep(1000);
        }
    }

    long lastAckReport = 0;
    long totalAcks = 0;
    @Override
    public void ack(Object msgId) {
        totalAcks++;
//        long now = System.currentTimeMillis();
//        if (now > lastAckReport + 500) {
//            System.out.println("ACK: " + msgId + " of " + totalAcks);
//            lastAckReport = now;
//        }
//        super.ack(msgId);    //To change body of overridden methods use File | Settings | File Templates.
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
//        System.out.println("SimpleKafkaSpout.fail: " + msgId);
//        super.fail(msgId);    //To change body of overridden methods use File | Settings | File Templates.
        pending.remove(msgId);
    }
}
