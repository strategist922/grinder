package org.jauntsy.grinder.karma.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.karma.Emitter;
import org.jauntsy.grinder.karma.KarmaTopologyBuilder;
import org.jauntsy.grinder.karma.KarmaConfig;
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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

import static org.jauntsy.nice.Nice.M;

/**
 * User: ebishop
 * Date: 1/2/13
 * Time: 9:50 AM
 */
public class _SnipeHunt {

    private static final String DEPOSITS = "deposits(id) { account amount }";
    private static final String WITHDRAWALS = "withdrawals(id) { account amount }";
    private static final String TRANSFERS = "transfers(id) { srcAccount dstAccount amount }";

    public static void main(String[] args) throws IOException {

        Logger.getRootLogger().setLevel(Level.WARN);

//        prepHbase();

        // *** start the storm cluster
        LocalCluster cluster = new LocalCluster();

        // *** start the embedded kafka service
        LocalKafkaBroker broker = new LocalKafkaBroker(0, 9090, 1, "localhost:2000");

        // *** configure replay and karma to use the local kafka instance
        ReplayConfig replay = new ReplayConfig().staticHosts(broker.getHostPortStrings(), broker.getNumPartitions());
//        KarmaConfig karmaConfig = new LocalKarmaConfig("a", cluster, broker, new HbaseReducerState());
        KarmaConfig karmaConfig = new KarmaConfigImpl("a", replay, new CassandraReducerState("snipehunt"));
//        KarmaConfig karmaConfig = new LocalKarmaConfig("a", cluster, broker, new InMemoryReducerState());

        KarmaTopologyBuilder karma = new KarmaTopologyBuilder(karmaConfig, "snipehunt");

//        builder.setSpout("orgSpout", new KafkaSpout(replay.buildReplaySpoutConfig("org", ORG_SCHEME, "orgSpoutId")), 4);
        karma.setSpout("orgSpout", new KafkaSpout(new ReplaySpoutConfig(replay, "org", JsonProducer.ORG_SCHEME, "orgSpoutId")), 4);
        karma.setSpout("userSpout", new KafkaSpout(new ReplaySpoutConfig(replay, "user", JsonProducer.USER_SCHEME, "userSpoutId")), 4);
        karma.setSpout("tweetSpout", new KafkaSpout(new ReplaySpoutConfig(replay, "tweets", new JsonScheme("id", "tweet"), "tweetSpoutId")), 4);

        FeederSpout feederSpout = new FeederSpout(new Fields("id", "account", "amount"));
        karma.setSpout("deposits", feederSpout, 1)
                .setMaxSpoutPending(100);
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

//        karma.select("deposits(id) { account amount }").using(new One("account"))
//             .select("withdrawals(id) { account amount }").using(new One("account"))
//             .select("transfers(id) { dstAccount amount }").using(new One("dstAccount"))
//             .select("transfers(id) { srcAccount amount }").using(new One("srcAccount"))
//             .as("account", "transactions").groupBy("account").red(new SumReducer()).build("transactionCounts");
//        buildSniffer(builder, "transactionCounts");

        cluster.submitTopology("mytopo", new Config(), karma.createTopology());



//        Producer<Long, Message> producer = broker.buildSyncProducer();
//        DS ds = new DS(producer);

        /*
        DS.Topic tweets = ds.newJsonTopic("tweets(id) { tweet }");
        tweets.update(7, "This is the last of the roman ruins");
        tweets.update(8, "It is the end of the world as we know it");
        tweets.update(7, null);
        tweets.update(8, "roman roman roman roman roman roman");

//        Demo.countdown("Sending users in ", 3);
//        Demo.countdown(0, "Adding Joe in ", 1);
        ds.sendUser(1, "Joe", 1001);
//        Demo.countdown("Adding Sue in ", 1);
        ds.sendUser(2, "Sue", 1001);
//        Demo.countdown("Renaming Joe to Joanne in ", 1);
        ds.sendUser(1, "Joanne", 1001);
//        Demo.countdown("Deleting Sue in ", 1);
        ds.deleteUser(2);
//        Demo.countdown("Deleting Joanne in ", 1);
        ds.deleteUser(1);
//        Utils.sleep(3000);
  */
//        DS.Topic deposits = ds.newJsonTopic("deposits(id) { account amount }");
//        DS.Topic withdrawals = ds.newJsonTopic("withdrawals(id) { account amount }");
//        DS.Topic transfers = ds.newJsonTopic("transfers(id) { srcAccount dstAccount amount }");

        // *** cleanup old data
//        for (int i = 0; i < 10; i++) {
//            deposits.delete(i);
//            withdrawals.delete(i);
//            transfers.delete(i);
//        }


        /*
//        Demo.countdown2(3000, "Deposits in", 3);
        deposits.update(1, 1001, 99.99);
        deposits.update(1, 1001, 99.99);
        deposits.update(1, 1001, 99.99);  // == 99.99
        deposits.update(2, 1001, 0.01);   // == 100.0
        deposits.update(2, 1001, 100.01); // == 200.0 (replaces tx #2)
        deposits.update(3, 1001, null);   // *** does nothing
        deposits.update(1, 1001, null);   // == 100.01 (removes tx #1)
        deposits.update(1, null, null);   // *** does nothing
        deposits.update(2, null, null);   // == null (removes tx #2)
        deposits.update(3, null, null);   // *** does nothing
*/

//        if (false) {
//            transferDemo(ds);
//        }

        Demo.countdown2("Crazy time", 5);
        for (int i = 0; i < 1; i++) {
//            for (int j = 0; j < 1000000; j++) {
            for (int j = 0; j < 1000000; j++) {
                feederSpout.feed(new Values(j, "p", 1.0f));
//                ds.sendJson(DEPOSITS, M("id", j, "account", "p", "amount", 1.0f));
//                deposits.update(M("id", j, "account", "p", "amount", null));
                Utils.sleep(1);
            }
        }

        Utils.sleep(100000);
//        producer.close();
    }

    private static void transferDemo(JsonProducer mb) {
        Demo.countdown2("Deposit(id:1) 100$ to account A in", 5);
        //        ds.sendJson("deposits(id) { account amount }", L(1, "a", 100.0f));
//            deposits.update(M("id", 1, "account", "a", "amount", 100.0));
        mb.update(DEPOSITS, M("id", 1, "account", "a", "amount", 100.0));
        Utils.sleep(3000);
        System.out.println();
        System.out.println();
        //        deposits.update(M("account", "a", "id", 1, "amount", 100.0));

        Demo.countdown2("Deposit(id:2) 50$ to account B in", 5);
        //        deposits.update("b", 2, 50.0);
        mb.update(DEPOSITS, M("id", 2, "account", "b", "amount", 50.0));
        Utils.sleep(3000);
        System.out.println();
        System.out.println();

        Demo.countdown2("Withdraw(id:1) 10$ from account A in", 5);
        //ds.sendJson("withdrawals(id) { account amount }", L(1, "a", 10.0));
        mb.update(WITHDRAWALS, M("id", 1, "account", "a", "amount", 10.0));
        Utils.sleep(3000);
        System.out.println();
        System.out.println();

        Demo.countdown2("Transfer(id:1) 10$ from account A to B in", 5);
        //        transfers.update(1, "a", "b", 10.0);
        mb.update(TRANSFERS, M("id", 1, "srcAccount", "a", "dstAccount", "b", "amount", 10.0));
        Utils.sleep(3000);
        System.out.println();
        System.out.println();

        Demo.countdown2("Invalidating previous transfer(id:1) in", 5);
        mb.delete(TRANSFERS, M("id", 1));
        Utils.sleep(3000);
        System.out.println();
        System.out.println();
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
        dropCreateTable(admin, tableName + "_map", false);
        dropCreateTable(admin, tableName + "_red", true);
    }

    private static void dropCreateTable(HBaseAdmin admin, String tableName, boolean bloom) throws IOException {
        try {
            while (admin.tableExists(tableName)) {
                System.out.println("Attempting to delete: " + tableName);
                try { admin.disableTable(tableName); } catch(Exception ignore) {}
                try { admin.deleteTable(tableName); } catch(Exception ignore) {}
            }
        } catch(Exception ignore) {
            System.out.println(ignore);
        }
        System.out.println("Creating table: " + tableName);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        HColumnDescriptor increments = new HColumnDescriptor("increments");
        if (bloom)
            increments.setBloomFilterType(StoreFile.BloomType.ROWCOL);
        desc.addFamily(increments);
        admin.createTable(desc);
    }

    private static void buildSniffer(KarmaTopologyBuilder builder, String srcComponentId) {
        buildSniffer(builder, srcComponentId, "default");
    }

    private static void buildSniffer(KarmaTopologyBuilder builder, String srcComponentId, String stream) {
        builder.setBolt(srcComponentId + "Sniffer", new PrintBolt("### " + srcComponentId, 1000, true)).globalGrouping(srcComponentId, stream);
    }


}
