package org.jauntsy.grinder.panama;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.panama.api.*;
import org.jauntsy.grinder.panama.contrib.snapshot.MongoSnapshotAdapter;
import org.jauntsy.grinder.panama.util.ZkHelper;
import org.jauntsy.grinder.replay.api.ArchiveConfig;
import org.jauntsy.grinder.replay.contrib.storage.mongo.MongoArchiveConfig;
import org.jauntsy.grinder.replay.contrib.storage.mongo.MongoDbProperties;
import org.jauntsy.nice.Time;
import com.mongodb.Mongo;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: ebishop
 * Date: 12/7/12
 * Time: 4:00 PM
 */
public class _2ClusterDemo {

    private static final int NUM_USERS = 1000;


    public static void main(String[] args) {
    }

    public static class Local {
        public static void main(String[] args) throws Exception {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        Utils.sleep(1000);
                        System.out.println(".");
                    }
                }
            }).start();
            Logger.getRootLogger().setLevel(Level.WARN);

            dropMongo();

            LocalCluster cluster = new LocalCluster();
            LocalKafkaBroker kafka = new LocalKafkaBroker(0, 9090, 4, "localhost:2000");

            ClusterConfig clusterA = configureCluster(kafka, "a");
            ClusterConfig clusterB = configureCluster(kafka, "b");

            // *** submit cluster A and cluster B to storm
            cluster.submitTopology("topo_a", new Config(), clusterA.buildClusterTopology());
            cluster.submitTopology("topo_b", new Config(), clusterB.buildClusterTopology());

            // *** start the replication from A into B
            cluster.submitTopology("repl_a2b", new Config(), clusterB.buildSubscriptionFrom(clusterA));

            // *** start a daemon to archive the user queue on cluster A
            cluster.submitTopology("a_user_archivist", new Config(), clusterA.buildQueueArchivistTopology("a_user"));

//            cluster.submitTopology("topoAUsers", new Config(), clusterA.buildTableTopology("user"));
//            cluster.submitTopology("userTable", new Config(), test.buildTable(new TableBuilder("user")));
//            new ViewBuilder("nameCounts").by("name")map("user", "{ d -> emit(d.name, 1) }").reduce("{ a, b -> a + b }");

            ClusterWriter writerA = clusterA.buildWriter();
            StateReader readerA = clusterA.buildReader(null);

            countdown("Insert 1st user in ", 3);
            if (true) {
                // *** 1
                System.out.println("Insert 1");
                writerA.put("user", new Dbo(14).append("name", "Joe7").append("age", 27), 1000);
                // *** 2
                System.out.println("Insert 2");
                writerA.put("user", new Dbo(14).append("name", "Joe6").append("age", 26).append("color", "green"), 999);
                long start = Time.now();
                Dbo user = null;
                while (null == (user = readerA.get("user", new Dbo(14)))) {
//                    System.out.println("Waiting for user 14 (" + Time.since(start) + " ms)");
                    Utils.sleep(10);
                }
                System.out.println("Found user 14 in " + Time.since(start) + " ms");
            }

            countdown("Insert 2nd user in ", 3);
            if (true) {
                // *** 1
                writerA.put("user", new Dbo(15).append("name", "Joe7").append("age", 27), 1000);
                // *** 2
                writerA.put("user", new Dbo(15).append("name", "Joe6").append("age", 26).append("color", "green"), 999);
                long start = Time.now();
                Dbo user = null;
                while (null == (user = readerA.get("user", new Dbo(15)))) {
//                    System.out.println("Waiting for user 14 (" + Time.since(start) + " ms)");
                    Utils.sleep(10);
                }
                System.out.println("Found user 15 in " + Time.since(start) + " ms");
            }

            if (true) {
                countdown("Starting bulk write in ", 5);
                long count = 0;
                long realCount = 0;
                long lastReport = Time.now();
                for (long i = 0; i < 100L; i++) {
                    List<Update> batch = new ArrayList<Update>();
                    count++;
                    for (int j = 0; j < NUM_USERS; j++) {
                        batch.add(new Update("user", new Dbo(j).append("cnt", count)));
                        realCount++;
                    }
                    writerA.putBatch(batch);
                    if (realCount % 5000 == 0) System.out.println("Wrote " + realCount + " updates");
                    Utils.sleep(100);
                }
                System.out.println("Wrote " + realCount + " updates");
                System.out.println("Waiting for doc 7 to reach cnt:100");
                long start = Time.now();
                while (true) {
                    Dbo d = readerA.get("user", new Dbo(7));
                    if (d != null) {
                        System.out.println(d);
                        Long cnt = d.getLongOrNull("cnt");
                        if (cnt != null && cnt >= 100L) {
                            System.out.println("Found in " + Time.since(start) + " ms");
                            break;
                        }
                    }
                    Utils.sleep(250);
                }
            }

            if (true) {
                long start;

                System.out.println("Writing user 1015");
                start = Time.now();
                writerA.put("user", new Dbo(1015).append("name", "name1015"));
                while(true) {
                    if (null != readerA.get("user", new Dbo(1015))) {
                        System.out.println("Found user 1015 in " + Time.since(start) + " ms");
                        break;
                    } else {
                        Utils.sleep(50);
                    }
                }

                Dbo user = new Dbo(14);

                start = Time.now();
                writerA.put("user", new Dbo(14).append("foo", "bar"));
                while(true) {
                    user = readerA.get("user", user);
                    if (user != null && user.getString("foo") != null) {
                        System.out.println("Found update of user 14 in " + Time.since(start) + " ms");
                        break;
                    } else {
                        Utils.sleep(25);
                    }
                }


                StateReader readerB = clusterB.buildReader(null);
                countdown("A to B sync test in ", 5);
                System.out.println("Creating user 1001 on cluster A, waiting for it to show on B");
                start = Time.now();
                writerA.put("user", new Dbo(1001).append("name", "user1001"));
                user = null;
                while (user == null) {
                    user = readerB.get("user", new Dbo(1001));
                    System.out.println("user 1001 on b = " + user);
                    Utils.sleep(25);
                }
                System.out.println("Found user 1001 on cluster B in " + Time.since(start) + " ms");
                readerB.close();
            }

            if (true) {
                countdown("User queue test in ", 5);
                SimpleConsumer consumer = kafka.buildSimpleConsumer();
                for (MessageAndOffset mno : consumer.fetch(new FetchRequest("a_user", 0, 0, 1000))) {
                    System.out.println(new String(Utils.toByteArray(mno.message().payload())));
                }
                consumer.close();
            }

            countdown("Checking queues in ", 3);

            System.out.println("a_incoming size: " + countQueueEntries(kafka, "a_incoming"));
            System.out.println(" a_changes size: " + countQueueEntries(kafka, "a_changes"));
            System.out.println("b_incoming size: " + countQueueEntries(kafka, "b_incoming"));
            System.out.println(" b_changes size: " + countQueueEntries(kafka, "b_changes"));

            if (true) {
                countdown("Dumping zookeeper in ", 5);
                ZooKeeper zk = ZkHelper.connect("localhost:2000", 100000);
                ZkHelper.dumpToConsole(zk, "/");
            }


            countdown("Exit in ", 100);

            cluster.shutdown();
            kafka.shutdown();
        }

        private static long countQueueEntries(LocalKafkaBroker kafka, String topic) {
            long total = 0;
            SimpleConsumer consumer = kafka.buildSimpleConsumer();
            for (int partition = 0; partition < kafka.getNumPartitions(); partition++) {
                long partitionSize = 0L;
                long offset = 0;
                while (true) {
                    long batchSize = 0;
                    for (MessageAndOffset mno : consumer.fetch(new FetchRequest(topic, partition, offset, 100000))) {
                        batchSize++;
                        partitionSize++;
                        total++;
                        offset = mno.offset();
                    }
                    if (batchSize == 0)
                        break;
                }
                System.out.println("--- " + topic + ":" + partition + " size: " + partitionSize);
            }
            consumer.close();
            return total;
        }


        private static void dropMongo() throws UnknownHostException {
            Mongo m = new Mongo("localhost");
            m.getDB("_replay").dropDatabase();
            m.getDB("a").dropDatabase();
            m.getDB("b").dropDatabase();
            m.close();
        }

        private static ClusterConfig configureCluster(LocalKafkaBroker broker, String ns) {

//            SnapshotConfig snapshot = new MemorySnapshotConfig(ns);
            MongoSnapshotAdapter.Config snapshot = new MongoSnapshotAdapter.Config(new MongoDbProperties.Builder().host("localhost").db(ns));

            ArchiveConfig archive = new MongoArchiveConfig.Builder().host("localhost");

            ClusterConfig cluster = new ClusterConfig(ns)
                    .staticHosts(new String[] { "localhost:9090" }, broker.getNumPartitions())
//                    .dynamicHosts("localhost:2000")
                    .zkConnect("localhost:2000")
                    .snapshot(snapshot)
                    .archive(archive)
                    .numCurators(broker.getNumPartitions());

            return cluster;
        }
    }

    private static void countdown(String prefix, int secs) {
        for (int i = secs; i > 0; i--) {
            System.out.println(prefix + i);
            Utils.sleep(1000);
        }
    }

    public static int rint(int range) {
        return (int)(Math.random() * range);
    }

}

