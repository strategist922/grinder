package com.exacttarget.kafka.config;

import org.jauntsy.nice.PropertiesBuilder;
import kafka.producer.Partitioner;
import kafka.serializer.Encoder;

/**
 * User: ebishop
 * Date: 12/14/12
 * Time: 10:51 AM
 */
public abstract class ProducerProperties<K,M,T extends ProducerProperties<K,M,?>> extends PropertiesBuilder<T> {

    public static final String BATCH_SIZE = "batch.size";
    public static final String BROKER_LIST = "broker.list";
    public static final String BUFFER_SIZE = "buffer.size";
    public static final String COMPRESSION_CODEC = "compression.codec";
    public static final String CONNECT_TIMEOUT_MS = "connect.timeout.ms";
    public static final String MAX_MESSAGE_SIZE = "max.message.size";
    public static final String PRODUCER_TYPE = "producer.type";
    public static final String PARTITIONER_CLASS = "partitioner.class";
    public static final String QUEUE_TIME = "queue.time";
    public static final String QUEUE_SIZE = "queue.size";
    public static final String RECONNECT_INTERVAL = "reconnect.interval";
    public static final String SERIALIZER_CLASS = "serializer.class";
    public static final String SOCKET_TIMEOUT_MS = "socket.timeout.ms";
    public static final String ZK_CONNECT = "zk.connect";
    public static final String ZK_READ_NUM_RETRIES = "zk.read.num.retries";

    public enum ProducerType  { SYNC, ASYNC }

    public T serializerClass(Class<? extends Encoder> clazz) {
        return append(SERIALIZER_CLASS, clazz.getName());
    }

    public T partitionerClass(Class<? extends Partitioner> clazz) {
        return append(PARTITIONER_CLASS, clazz.getName());
    }

    /*
    eg. broker.list=0:localhost:9090,1:foo.bar.muu:9091 (TODO: double check this example)
     */
    public T brokerList(String brokerList) {
        return append(BROKER_LIST, brokerList);
    }

//    public T zkConnect(String zkConnect) {
//        return append("zk.connect", zkConnect);
//    }

    public T compressedTopics(String... topics) {
        throw new UnsupportedOperationException();
    }

    public T compressionCodec(int codec) {
        return append(COMPRESSION_CODEC, codec); // def 0
    }

    public T zkConnect(String zkConnect) {
        return append(ZK_CONNECT, zkConnect);
    }

    public T zkReadNumRetries(int retries) {
        return append(ZK_READ_NUM_RETRIES, retries);
    }

    public T bufferSize(int sizeInBytes) {
        return append(BUFFER_SIZE, sizeInBytes); // def 102400
    }

    public T maxMessageSize(int sizeInBytes) {
        return append(MAX_MESSAGE_SIZE, sizeInBytes); // *** def 1000000
    }

    public T connectTimeoutMs(long millis) {
        return append(CONNECT_TIMEOUT_MS, millis); // def 5000
    }

    public T socketTimeoutMs(long millis) {
        return append(SOCKET_TIMEOUT_MS, millis);
    }

    public T reconnectInterval(int requests) { // *** def 30000
        return append(RECONNECT_INTERVAL, requests); // *** def 3000
    }

    public T producerTypeSYNC() {
        return producerType(ProducerType.SYNC);
    }

    public T producerTypeASYNC() {
        return producerType(ProducerType.ASYNC);
    }

    public T producerType(ProducerType type) {
        switch (type) {
            case SYNC:
                return append(PRODUCER_TYPE, "sync");
            case ASYNC:
                return append(PRODUCER_TYPE, "async");
            default:
                throw new IllegalArgumentException("type: " + type);
        }
    }

}
