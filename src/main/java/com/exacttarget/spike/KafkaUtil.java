package com.exacttarget.spike;

import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;

import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * User: ebishop
 * Date: 11/12/12
 * Time: 5:02 PM
 */
public class KafkaUtil {

    public static final String KAFKA_DEFAULT_ZK_CONNECT_STRING = "localhost:2181";

    public static final String KAFKA_DEFAULT_HOST = "localhost";
    public static final int KAFKA_DEFAULT_PORT = 9092;
    public static final int KAFKA_DEFAULT_SO_TIMEOUT = 100000;
    public static final int KAFKA_DEFAULT_BUFFER_SIZE = 64 * 1000;

    public static final String KAFKA_DEFAULT_HOSTPORT = KAFKA_DEFAULT_HOST + ":" + KAFKA_DEFAULT_PORT;

    public static long getOffsetBeforeEarliestTime(SimpleConsumer consumer, String topic, int partition) {
        return getOffsetBeforeTime(consumer, topic, partition, OffsetRequest.EarliestTime());
    }

    public static long getOffsetBeforeLatestTime(SimpleConsumer consumer, String topic, int partition) {
        return getOffsetBeforeTime(consumer, topic, partition, OffsetRequest.LatestTime());
    }

    public static long getOffsetBeforeTime(SimpleConsumer consumer, String topic, int partition, long time) {
        long[] offsets = consumer.getOffsetsBefore(topic, partition, time, 1);
        return offsets == null || offsets.length == 0 ? 0L : offsets[0];
    }

    public static SimpleConsumer newSimpleConsumer() {
        return new SimpleConsumer(KAFKA_DEFAULT_HOST, KAFKA_DEFAULT_PORT, KAFKA_DEFAULT_SO_TIMEOUT, KAFKA_DEFAULT_BUFFER_SIZE);
    }

    public static byte[] copyBytes(Message message) {
        ByteBuffer payload = message.payload();
        byte[] bytes = new byte[payload.remaining()];
        payload.get(bytes);
        return bytes;
    }

    public static KissProducer newKissProducer() {
        return new KissProducer();
    }

    public static KissProducer newKissProducer(String zkConnectString) {
        return new KissProducer(zkConnectString, false);
    }

    public static class KissProducer {

        private Producer<Integer, Message> producer;

        public KissProducer() {
            this(null, false);
        }

        public KissProducer(String zkConnectString, boolean async) {
            zkConnectString = zkConnectString == null ? "localhost:2181" : zkConnectString;
            Properties props = new Properties();
            props.put("zk.connect", zkConnectString);
            if (async)
                props.put("producer.type", "async");
            this.producer = new Producer<Integer, Message>(new ProducerConfig(props));
        }

        public void send(String topic, byte[] payload) {
//            StringEncoder encoder = new StringEncoder();
            Message message = new Message(payload);
            ProducerData<Integer, Message> data = new ProducerData<Integer, Message>(topic, message);
            producer.send(data);
        }

        public void close() {
            producer.close();
        }

        public Producer<Integer, Message> getProducer() {
            return this.producer;
        }
    }

}
