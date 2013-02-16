package org.jauntsy.grinder.karma.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.karma.Emitter;import org.jauntsy.grinder.karma.KarmaTopologyBuilder;import org.jauntsy.grinder.karma.KarmaConfig;
import org.jauntsy.grinder.karma.KarmaConfigImpl;
import org.jauntsy.grinder.karma.contrib.cassandra.CassandraReducerState;
import org.jauntsy.grinder.karma.mapred.Mapper;
import org.jauntsy.grinder.karma.operations.map.Count;
import org.jauntsy.grinder.karma.operations.reduce.MinValue;
import org.jauntsy.grinder.karma.operations.reduce.Sum;
import org.jauntsy.grinder.karma.testing.PrintBolt;
import org.jauntsy.grinder.replay.api.ReplayConfig;
import org.jauntsy.grinder.replay.api.ReplaySpoutConfig;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaSpout;
import org.jauntsy.nice.Demo;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

import static org.jauntsy.nice.Nice.L;
import static org.jauntsy.nice.Nice.M;

/**
 * User: ebishop
 * Date: 1/2/13
 * Time: 9:50 AM
 */
public class _ReduceNoneTest {

    public static void main(String[] args) throws IOException {

        Logger.getRootLogger().setLevel(Level.WARN);

//        prepHbase();

        // *** start the storm cluster
        LocalCluster cluster = new LocalCluster();

        // *** start the embedded kafka service
        LocalKafkaBroker broker = new LocalKafkaBroker(0, 9090, 1, "localhost:2000");

        // *** configure replay and karma to use the local kafka instance
        ReplayConfig replay = new ReplayConfig().staticHosts(broker.getHostPortStrings(), broker.getNumPartitions());
//        KarmaConfig karmaConfig = new LocalKarmaConfig("a", cluster, broker, new InMemoryReducerState());
        KarmaConfig karmaConfig = new KarmaConfigImpl("a", replay, new CassandraReducerState("reducenonetest"));

        KarmaTopologyBuilder karma = new KarmaTopologyBuilder(karmaConfig, "rednonetest");

//        builder.setSpout("orgSpout", new KafkaSpout(replay.buildReplaySpoutConfig("org", ORG_SCHEME, "orgSpoutId")), 4);
        karma.setSpout("orgSpout", new KafkaSpout(new ReplaySpoutConfig(replay, "org", JsonProducer.ORG_SCHEME, "orgSpoutId")), 4);
        karma.setSpout("userSpout", new KafkaSpout(new ReplaySpoutConfig(replay, "user", JsonProducer.USER_SCHEME, "userSpoutId")), 4);
        karma.setSpout("tweetSpout", new KafkaSpout(new ReplaySpoutConfig(replay, "tweets", new JsonScheme("id", "tweet"), "tweetSpoutId")), 4);

        karma.setSpout("deposits", new KafkaSpout(replay.buildReplaySpoutConfig("deposits", new JsonScheme("id", "account", "amount"), "depSpoutId")), 4).setMaxSpoutPending(100);
        karma.setSpout("withdrawals", new KafkaSpout(replay.buildReplaySpoutConfig("withdrawals", new JsonScheme("id", "account", "amount"), "widthSpoutId")), 4);
        karma.setSpout("transfers", new KafkaSpout(replay.buildReplaySpoutConfig("transfers", new JsonScheme("id", "srcAccount", "dstAccount", "amount"), "transSpoutId")), 4);

        karma.map("{ orgId, id, name }", "userSpout(id)").red("{ userName }", "orgidUseridToUsername(orgId userId)", new MinValue());
        buildSniffer(karma, "orgidUseridToUsername");

        karma.map("{ name }", "userSpout(id)", new Count()).red("{ count }", "userCount()", new Sum());
        buildSniffer(karma, "userCount");

        karma.map("{ tweet }", "tweetSpout(id)", new Mapper() {
            @Override
            public void map(Tuple doc, Emitter e) {
                System.out.println("_ReduceNoneTest.map");
                for (String word : doc.getString(0).split(" "))
                    e.emit(word.toLowerCase(), 1);
            }
        }).red("{ count }", "wordCount(word)", new Sum());
        buildSniffer(karma, "wordCount");

        karma.map("{ account amount }", "deposits(id)")
             .map("{ account amount }", "withdrawals(id)", "{ d -> emit(d.account, -d.amount) }")
             .map("{ dstAccount amount }", "transfers(id)")
             .map("{ srcAccount amount }", "transfers(id)", "{ d -> emit(d.srcAccount, -d.amount) }")
             .red("{ balance }", "accountBalance(account)", new Sum());
        buildSniffer(karma, "accountBalance");

//        karma.map("{ account amount }", "deposits(id)", new OnePer("account"))
//             .map("{ account amount }", "withdrawals(id)", new OnePer("account"))
//             .map("{ dstAccount amount }", "transfers(id)", new OnePer("dstAccount"))
//             .map("{ srcAccount amount }", "transfers(id)", new OnePer("srcAccount"))
//             .red("{ transactions }", "transactionCounts(account)", new Sum()).build();
//        buildSniffer(builder, "transactionCounts");

        cluster.submitTopology("mytopo", new Config(), karma.createTopology());



        Producer<Long, Message> producer = broker.buildSyncProducer();
        JsonProducer mb = new JsonProducer(producer);

        if (false) {
        mb.update("tweets(id)", M("id", 7, "tweet", "This is the last of the roman ruins"));
        mb.update("tweets(id)", M("id", 8, "tweet", "It is the end of the world as we know it"));
        mb.update("tweets(id)", M("id", 7, "tweet", null));
        mb.update("tweets(id)", M("id", 8, "tweet", "roman roman roman roman roman roman"));

//        Demo.countdown("Sending users in ", 3);
//        Demo.countdown(0, "Adding Joe in ", 1);
        mb.sendUser(1, "Joe", 1001);
//        Demo.countdown("Adding Sue in ", 1);
        mb.sendUser(2, "Sue", 1001);
//        Demo.countdown("Renaming Joe to Joanne in ", 1);
        mb.sendUser(1, "Joanne", 1001);
//        Demo.countdown("Deleting Sue in ", 1);
        mb.deleteUser(2);
//        Demo.countdown("Deleting Joanne in ", 1);
        mb.deleteUser(1);
//        Utils.sleep(3000);

//        Demo.countdown2(3000, "Deposits in", 3);
        mb.update("deposits(id)", L("id", "account", "amount"), L(1, 1001, 99.99));
        mb.update("deposits(id)", L("id", "account", "amount"), L(1, 1001, 99.99));
        mb.update("deposits(id)", L("id", "account", "amount"), L(1, 1001, 99.99));  // == 99.99
        mb.update("deposits(id)", L("id", "account", "amount"), L(2, 1001, 0.01));   // == 100.0
        mb.update("deposits(id)", L("id", "account", "amount"), L(2, 1001, 100.01)); // == 200.0 (replaces tx #2)
        mb.update("deposits(id)", L("id", "account", "amount"), L(3, 1001, null));   // *** does nothing
        mb.update("deposits(id)", L("id", "account", "amount"), L(1, 1001, null));   // == 100.01 (removes tx #1)
        mb.update("deposits(id)", L("id", "account", "amount"), L(1, null, null));   // *** does nothing
        mb.update("deposits(id)", L("id", "account", "amount"), L(2, null, null));   // == null (removes tx #2)
        mb.update("deposits(id)", L("id", "account", "amount"), L(3, null, null));   // *** does nothing

//        Demo.countdown2("Deposit(id:1) 100$ to account A in", 5);
        mb.update("deposits(id)", M("id", 1, "account", "a", "amount", 100.0));

//        Demo.countdown2("Deposit(id:2) 50$ to account B in", 5);
        mb.update("deposits(id)", M("id", 2, "account", "b", "amount", 50.0));

//        Demo.countdown2("Withdraw(id:1) 10$ from account A in", 5);
        mb.update("withdrawals(id)", M("id", 1, "account", "a", "amount", 10.0));


//        Demo.countdown2("Transfer(id:1) 10$ from account A to B in", 5);
        mb.update("transfers(id)", M("id", 1, "srcAccount", "a", "dstAccount", "b", "amount", 10.0));

//        Demo.countdown2("Invalidating previous transfer(id:1) in", 5);
        mb.update("transfers(id)", L("id", "srcAccount", "dstAccount", "amount"), L(1, null, null, null));
        }

        Demo.countdown2("Crazy time", 5);
        for (int i = 0; i < 1000000; i++) {
            mb.update("deposits(id)", M("id", i, "account", "a", "amount", 1.0));
            Utils.sleep(1);
        }

        Utils.sleep(100000);
        producer.close();
    }

    private static void prepHbase() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
        dropCreateMapRedTables(admin, "accountBalance");
        dropCreateMapRedTables(admin, "wordCount");
        dropCreateMapRedTables(admin, "orgidUseridToUsername");
        dropCreateMapRedTables(admin, "transactionCounts");
        dropCreateMapRedTables(admin, "userCount");
        admin.close();
    }

    private static void dropCreateMapRedTables(HBaseAdmin admin, String tableName) throws IOException {
        dropCreateTable(admin, tableName + "_map");
        dropCreateTable(admin, tableName + "_red");
    }

    private static void dropCreateTable(HBaseAdmin admin, String tableName) throws IOException {
        try {
            while (admin.tableExists(tableName)) {
                System.out.println("Attempting to delete: " + tableName);
                try { admin.disableTable(tableName); } catch(Exception ignore) {}
                try { admin.deleteTable(tableName); } catch(Exception ignore) {}
            }
        } catch(Exception ignore) {
            System.out.println(ignore);
        }
        HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.addFamily(new HColumnDescriptor("increments"));
        admin.createTable(desc);
    }

    private static void buildSniffer(KarmaTopologyBuilder builder, String srcComponentId) {
        builder.setBolt(srcComponentId + "Sniffer", new PrintBolt("### " + srcComponentId, 1000, true)).globalGrouping(srcComponentId, "changes");
    }


}
