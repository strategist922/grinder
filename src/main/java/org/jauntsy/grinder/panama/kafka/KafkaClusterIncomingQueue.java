package org.jauntsy.grinder.panama.kafka;

import backtype.storm.topology.IRichSpout;
import org.jauntsy.grinder.panama.api.*;
import org.jauntsy.grinder.panama.kafka.scheme.UpdateScheme;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaSpout;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.SpoutConfig;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
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
public class KafkaClusterIncomingQueue implements ClusterIncomingQueue {

    String topic;
    Properties producerProperties;
    KafkaSpout kafkaSpout;

    UpdateScheme scheme;

    public KafkaClusterIncomingQueue(String ns, Map producerProperties, SpoutConfig spoutConfig) {
        this.topic = ns + "_incoming";
        this.producerProperties = new Properties();
        if (producerProperties != null)
            this.producerProperties.putAll(producerProperties);
        this.kafkaSpout = new KafkaSpout(spoutConfig);
        this.scheme = new UpdateScheme();
    }

    @Override
    public ClusterWriter buildWriter() {
        final Producer<Long,Message> producer = new Producer<Long,Message>(new ProducerConfig(producerProperties));
        return new BaseClusterWriter() {
            @Override
            public void putOne(Update update) {
                Message msg = new Message(scheme.serialize(update));
                ProducerData data = new ProducerData<Long, Message>(topic, getKey(update.value.getId()), Arrays.asList(msg));
                producer.send(data);
            }

            @Override
            public void putBatch(List<Update> batch) {
                List<ProducerData<Long,Message>> producerData = new ArrayList<ProducerData<Long,Message>>();
                for (Update e : batch) {
                    Dbo value = e.value;
                    Long routingKey = getKey(value.getId());
                    Message msg = new Message(scheme.serialize(e));
                    producerData.add(new ProducerData<Long, Message>(topic, routingKey, Arrays.asList(msg)));
                }
                producer.send(producerData);
            }

            @Override
            public void cleanup() {
                producer.close();
            }
        };
    }

    @Override
    public IRichSpout buildSpout(String id) {
        return kafkaSpout;
    }

    public static long getKey(List id) {
        Hasher hasher = Hashing.md5().newHasher();
        for (Object o : id) {
            if (o == null) {
                // do nothing
            } else if (o instanceof String) {
                hasher.putString((String)o);
            } else if (o instanceof Long) {
                hasher.putLong((Long)o);
            } else if (o instanceof Integer) {
                hasher.putInt((Integer) o);
            } else {
                throw new IllegalArgumentException("Type: " + o.getClass() + ", value: " + o);
            }
        }
        return hasher.hash().asLong();
    }
}
