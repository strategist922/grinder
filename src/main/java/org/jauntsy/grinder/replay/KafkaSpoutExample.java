package org.jauntsy.grinder.replay;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import clojure.lang.Keyword;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.panama.util.ZkHelper;
import org.jauntsy.grinder.replay.api.*;
import org.jauntsy.grinder.replay.contrib.storage.mongo.MongoArchiveConfig;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.*;
import com.mongodb.*;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * User: ebishop
 * Date: 12/17/12
 * Time: 5:13 PM
 */
public class KafkaSpoutExample {

    static boolean DROP_MONGO = true;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        Logger.getRootLogger().setLevel(Level.WARN);

        if (DROP_MONGO) {
            Mongo mongo = new Mongo("localhost");
            mongo.getDB("replay_example").dropDatabase();
            mongo.close();
        }

        // *** creates a local cluster with zookeeper running on localhost:2000
        LocalCluster cluster = new LocalCluster();
//        dumpState(cluster);

        // *** creates a local broker running on localhost:9090
        LocalKafkaBroker kafka = buildLocalKafkaBroker();

        send100TestMessages(kafka);

        ArchiveConfig archive = new MongoArchiveConfig.Builder().host("localhost").db("replay_example");

        ReplayTopicConfig kisses = new ReplayTopicConfig.Builder()
                .dynamicHosts("localhost:2000")
                .topic("kisses")
                .archive(archive);


        countdown("Start an 'archivist' topology in storm which stores all the events from queue:'kisses' into mongodb: in ", 3);
        startTopicArchivistTopology(cluster, kisses);

        countdown("Consume the events with storm: in ", 3);
        demoStormConsumer(cluster, kisses);

        countdown("Consume the events from the DB via the adapter: in ", 5);
        demoArchiveConsumer(archive);

        countdown("Consume the events directly from Mongodb: in ", 3);
        demoMongodbConsumer();

        countdown("Consume the events with Kafka SimpleConsumer: in ", 3);
        demoSimpleConsumer(archive);


        dumpZookeeperToConsole();


        countdown("Shutting down in ", 10);
        kafka.shutdown();
        cluster.shutdown();

    }

    private static void send100TestMessages(LocalKafkaBroker kafka) throws UnsupportedEncodingException {
        // *** send 100 messages
        Producer<Long,Message> producer = kafka.buildSyncProducer();
        for (int i = 0; i < 100; i++) {
            String msg = "Kiss " + i;
            producer.send(new ProducerData<Long,Message>("kisses", new Message(msg.getBytes("UTF-8"))));
        }
        producer.close();
    }

    private static LocalKafkaBroker buildLocalKafkaBroker() throws IOException {
        //        LocalKafkaBroker kafka = new LocalKafkaBroker(0, "localhost:2000", 15000L);
        return new LocalKafkaBroker.Builder()
                .brokerid(0)
                .zkConnect("localhost:2000")
                .numPartitions(4)
                .buildLocalKafkaBroker();
    }

    private static void dumpZookeeperToConsole() throws IOException, InterruptedException, KeeperException {
        countdown("Dumping ZK in ", 5);
        ZooKeeper zk = ZkHelper.connect("localhost:2000", 100000);
        ZkHelper.dumpToConsole(zk, "/");
    }

    private static void startTopicArchivistTopology(LocalCluster cluster, ReplayTopicConfig kisses) {
        cluster.submitTopology("kissesArchivist", new Config(), kisses.buildTopicArchiveTopology(100));
    }

    private static void demoStormConsumer(LocalCluster cluster, ReplayTopicConfig kisses) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kissesSpout", new KafkaSpout(
                new ReplaySpoutConfig(kisses, "kissConsumerA")
                        .scheme(new StringScheme())
                        .forceFromStart()
        ));
        builder.setBolt("kissesBolt", new DebugBolt("kisses"), 2).shuffleGrouping("kissesSpout");
        cluster.submitTopology("kissConsumerA", new Config(), builder.createTopology());
    }

    private static void demoArchiveConsumer(ArchiveConfig archive) throws UnsupportedEncodingException {
        ArchiveClient archiveClient = archive.buildArchive(null);
        ArchiveClient.Partition partition = archiveClient.getPartition("kisses", "localhost", 9090, 0);
        long offset = 0L;
        for (MessageAndOffset mno : partition.fetch(offset, 100000)) {
            System.out.println("from archive: " + new String(Utils.toByteArray(mno.message().payload()), "UTF-8"));
            offset = mno.offset();
        }
        archiveClient.close();
    }

    private static void demoMongodbConsumer() throws UnknownHostException {
        Mongo mongo = new Mongo("localhost");
        DB db = mongo.getDB("replay_example");
        DBCollection coll = db.getCollection("kisses");
        for (DBObject dbo : coll.find()) {
            System.out.println("Mongodb: " + dbo);
        }
    }

    private static void demoSimpleConsumer(ArchiveConfig archive) {
        long offset;
        SimpleConsumer consumer = new SimpleReplayConsumer("localhost", 9090, 100000, 100000, archive.buildArchive(null));
        // *** use a VERY small batch size of 64 to force multiple passes
        offset = 0L;
        boolean exit = false;
        while (!exit) {
            exit = true;
            System.out.println("Fetch: " + offset);
            for (MessageAndOffset mno : consumer.fetch(new FetchRequest("kisses", 0, offset, 64))) {
                System.out.println("Simple consumer: " + new String(Utils.toByteArray(mno.message().payload())));
                offset = mno.offset();
                exit = false;
            }
        }
        consumer.close();
    }

    private static void dumpState(LocalCluster cluster) {
        Map state = cluster.getState();
        for (Keyword keyword : (Set<Keyword>)state.keySet()) {
            System.out.println("" + keyword + ": " + state.get(keyword));
            if(keyword.toString().equals(":daemon-conf")) {
                Map conf = new TreeMap((Map)state.get(keyword));
                for (String key : (Set<String>)conf.keySet()) {
                    System.out.println("\t" + key + ": " + conf.get(key));
                }
            }
        }

    }

    private static void countdown(String msg, int secs) {
        if (secs <= 0) return;
        for (int i = secs; i > 0; i--) {
            System.out.println(msg + i);
            Utils.sleep(1000);
        }
    }

    public static class DebugBolt extends BaseRichBolt {

        private final String name;

        private transient OutputCollector collector;

        public DebugBolt(String name) {
            this.name = name;
        }

        @Override
        public void prepare(Map config, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            System.out.println("*** " + name + ": [" + Thread.currentThread().getName() + "] fields: " + tuple.getValues() + " ***");
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

    }}
