package com.exacttarget.kafka.testing;

import backtype.storm.utils.Utils;
import com.exacttarget.kafka.KafkaBroker;
import org.jauntsy.grinder.panama.kafka.KafkaBrokerProperties;
import org.jauntsy.nice.Files;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import storm.kafka.HostPort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * User: ebishop
 * Date: 12/14/12
 * Time: 9:33 AM
 */
public class LocalKafkaBroker implements KafkaBroker {

    private static final int DEFAULT_SO_TIMEOUT = 10000;
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private KafkaBrokerProperties config;
    private KafkaConfig kafkaConfig;
    private KafkaServer server;

    public LocalKafkaBroker() throws IOException {
        init(getDefaultConfig());
    }

    public LocalKafkaBroker(int brokerId, int port, int numPartitions) throws IOException {
        KafkaBrokerProperties props = getDefaultConfig();
        props.brokerid(brokerId);
        props.port(port);
        props.numPartitions(numPartitions);
        init(props);
    }

    public LocalKafkaBroker(int brokerId, int port, int numPartitions, String zkConnect) throws IOException {
        this(brokerId, port, numPartitions, zkConnect, 100000L);
    }

    public LocalKafkaBroker(int brokerId, int port, int numPartitions, String zkConnect, Long zkConnectiontimeoutMs) throws IOException {
        KafkaBrokerProperties props = getDefaultConfig();
        props.brokerid(brokerId);
        props.port(port);
        props.numPartitions(numPartitions);
        if (zkConnect != null) {
            props.enableZookeeper(true);
            props.zkConnect(zkConnect);
            props.zkConnectiontimeoutMs(zkConnectiontimeoutMs);
        } else {
            props.enableZookeeper(false);
        }
        init(props);
    }

    public LocalKafkaBroker(KafkaBrokerProperties props) throws IOException {
        KafkaBrokerProperties derivedProps = getDefaultConfig();
        derivedProps.putAll(props);
        init(derivedProps);
    }

    private KafkaBrokerProperties getDefaultConfig() throws IOException {
        return new KafkaBrokerProperties.Builder()
                .brokerid(0)
                .hostname("localhost")
                .port(9090)
                .numPartitions(1)
                .enableZookeeper(false)
                .logFlushInterval(10000)
                .logDefaultFlushIntervalMs(200)
                .logDefaultFlushSchedulerIntervalMs(200)
                .logDir(Files.createTmpDir("embeddedkafka").getAbsolutePath());
    }

    private void init(KafkaBrokerProperties config) throws IOException {
        this.config = config;
        kafkaConfig = new KafkaConfig(config);
        server = new KafkaServer(kafkaConfig);
        server.startup();
    }

    public void shutdown() {
        server.shutdown();
    }

    public SimpleConsumer buildSimpleConsumer() {
        return new SimpleConsumer(
                config.getProperty("hostname"),
                Integer.parseInt(config.getProperty("port")),
                DEFAULT_SO_TIMEOUT,
                DEFAULT_BUFFER_SIZE
        );
    }

    public Producer<Long,Message> buildSyncProducer() {
        Properties config = new Properties();
        config.setProperty("producer.type", "sync");
        config.setProperty("broker.list", String.format("%s:%s:%d", getBrokerId(), getHostname(), getPort()));
        return new Producer<Long,Message>(new ProducerConfig(config));
    }

    public Producer<Long,Message> buildAsyncProducer() {
        Properties config = new Properties();
        config.setProperty("producer.type", "async");
        config.setProperty("queue.time", "2000");
        config.setProperty("queue.size", "100");
        config.setProperty("batch.size", "10");
        config.setProperty("broker.list", String.format("%s:%s:%d", getBrokerId(), getHostname(), getPort()));
        return new Producer<Long,Message>(new ProducerConfig(config));
    }

    public static void main(String[] args) throws IOException {
        LocalKafkaBroker localKafkaBroker = new LocalKafkaBroker();

        Producer<Long,Message> producer = localKafkaBroker.buildSyncProducer();
        SimpleConsumer consumer = localKafkaBroker.buildSimpleConsumer();

        producer.send(new ProducerData<Long,Message>("test", new Message("Hello, world!".getBytes())));
        Utils.sleep(4000);

        ByteBufferMessageSet fetch = consumer.fetch(new FetchRequest("test", 0, 0, 100000));
        System.out.println("fetch = " + fetch);
        for (MessageAndOffset mno : fetch) {
            long offset = mno.offset();
            Message msg = mno.message();
            System.out.println(kafka.utils.Utils.toString(msg.payload(), "UTF-8"));
        }

        localKafkaBroker.shutdown();
    }

    public int getBrokerId() {
        return Integer.parseInt(config.getProperty("brokerid"));
    }

    public String getHostname() {
        return config.getProperty("hostname");
    }

    public int getPort() {
        return Integer.parseInt(config.getProperty("port"));
    }

    public HostPort getHostPort() {
        return new HostPort(getHostname(), getPort());
    }

    public String getHostPortString() {
        return getHostname() + ":" + getPort();
    }

    /*
    @return ["localhost:9090"]
     */
    public List<String> getHostPortStrings() {
        List<String> ret = new ArrayList<String>();
        ret.add(getHostPortString());
        return ret;
    }

    public int getNumPartitions() {
        return Integer.parseInt(config.getProperty("num.partitions"));
    }

    public String getZkConnect() {
        return config.getProperty("zk.connect");
    }

    public static abstract class Config<T extends Config> extends KafkaBrokerProperties<T> {

    }

    public static class Builder extends Config<Builder> {
        public LocalKafkaBroker buildLocalKafkaBroker() throws IOException {
            return new LocalKafkaBroker(this);
        }
    }
}
