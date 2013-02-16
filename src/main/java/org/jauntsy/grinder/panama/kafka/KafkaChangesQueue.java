package org.jauntsy.grinder.panama.kafka;

import backtype.storm.topology.IRichSpout;
import org.jauntsy.grinder.panama.api.ChangesQueue;
import org.jauntsy.grinder.panama.api.ChangesWriter;
import org.jauntsy.grinder.panama.api.Change;
import org.jauntsy.grinder.panama.kafka.scheme.ArchiveScheme;
import org.jauntsy.grinder.replay.api.ReplayTopicConfig;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaScheme;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaSpout;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.SpoutConfig;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;

import java.util.*;

/**
 * User: ebishop
 * Date: 12/14/12
 * Time: 10:25 AM
 */
public class KafkaChangesQueue implements ChangesQueue {

    String ns;
    ReplayTopicConfig topicConfig;
    Properties producerProperties;

    public KafkaChangesQueue(String ns, ReplayTopicConfig topicConfig, Properties producerProperties) {
        this.topicConfig = topicConfig;
        this.ns = ns;
        this.producerProperties = producerProperties;
    }

    @Override
    public ChangesWriter buildWriter() {
        return new ChangesWriter() {

            private transient Producer<Long, Message> producer;
            private transient ArchiveScheme scheme;

            @Override
            public void prepare(Map stormConfig) {
                Properties config = new Properties();
                config.putAll(producerProperties);
                this.producer = new Producer<Long,Message>(new ProducerConfig(producerProperties));
                this.scheme = new ArchiveScheme();
            }

            @Override
            public void cleanup() {
                this.producer.close();
            }

            @Override
            public void putOne(Change change) {
                byte[] bytes = scheme.serialize(change);
                Message msg = new Message(bytes);
                producer.send(new ProducerData<Long, Message>(topicConfig.getTopic(), buildRoutingKey(change.getKey()), Arrays.asList(msg)));
            }

            @Override
            public void putBatch(List<Change> batch) {
                List<ProducerData<Long,Message>> producerData = new ArrayList<ProducerData<Long, Message>>();
                for (Change e : batch) {
                    byte[] bytes = scheme.serialize(e);
                    Message msg = new Message(bytes);
                    Long routingKey = buildRoutingKey(e.getKey());
                    producerData.add(new ProducerData<Long, Message>(topicConfig.getTopic(), routingKey, Arrays.asList(msg)));
                    producerData.add(new ProducerData<Long, Message>(ns + "_" + e.table, routingKey, Arrays.asList(msg)));
                }
                producer.send(producerData);
            }

            @Override
            public String toString() {
                return "KafkaChangesQueue(" + topicConfig.getTopic() + ")";
            }
        };
    }

    @Override
    public IRichSpout buildSpout(String id) {
        SpoutConfig spoutConf = topicConfig.buildReplaySpoutConfig(id);
        spoutConf.kafkaScheme = new KafkaScheme(new ArchiveScheme());
        return new KafkaSpout(spoutConf);
    }

    private static Long buildRoutingKey(List docId) {
        return KafkaClusterIncomingQueue.getKey(docId);
    }

}
