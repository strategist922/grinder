package org.jauntsy.grinder.replay.impl;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.replay.api.ArchiveClient;
import org.jauntsy.grinder.replay.api.ArchiveConfig;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.util.Map;

/**
 * User: ebishop
 * Date: 12/18/12
 * Time: 5:25 PM
 */
public class ArchivistBolt extends BaseRichBolt {

    private ArchiveConfig archiveConfig;

    private transient ArchiveClient archive;
    private transient OutputCollector collector;

    public ArchivistBolt(ArchiveConfig archiveConfig) {
        this.archiveConfig = archiveConfig;
    }

    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.archive = archiveConfig.buildArchive(config);
    }

    @Override
    public void cleanup() {
        this.archive.close();
    }

    @Override
    public void execute(Tuple tuple) {
//        System.out.println("ArchivistBolt values: " + tuple.getValues());
//        String topic = tuple.getStringByField("topic");
//        String hostname = tuple.getStringByField("hostname");
//        int port = tuple.getIntegerByField("port");
//        int partition = tuple.getIntegerByField("partition");
//        archive.getPartition(topic, hostname, port, partition).
//        long offset = 0L;
//        Message msg = new Message();
//        new MessageAndOffset(msg, offset);
        String topic = tuple.getStringByField("topic");
        String hostname = tuple.getStringByField("hostname");
        int port = tuple.getIntegerByField("port");
        int partition = tuple.getIntegerByField("partition");
        long offset = tuple.getLongByField("offset");
        byte[] bytes = tuple.getBinaryByField("bytes");
        long nextOffset = tuple.getLongByField("nextOffset");
        this.archive.getPartition(topic, hostname, port, partition).send(offset, new MessageAndOffset(new Message(bytes), nextOffset));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
