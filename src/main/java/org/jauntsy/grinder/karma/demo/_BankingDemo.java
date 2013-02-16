package org.jauntsy.grinder.karma.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.karma.KarmaTopologyBuilder;
import org.jauntsy.grinder.karma.KarmaConfig;
import org.jauntsy.grinder.karma.KarmaConfigImpl;
import org.jauntsy.grinder.karma.contrib.cassandra.CassandraPublisher;
import org.jauntsy.grinder.karma.contrib.cassandra.CassandraReducerState;
import org.jauntsy.grinder.karma.contrib.couchdb.CouchDbPublisher;
import org.jauntsy.grinder.karma.demo.bolts.DucksBoardCounterBolt;
import org.jauntsy.grinder.karma.demo.bolts.DucksBoardLeaderboardBolt;
import org.jauntsy.grinder.karma.mapred.Schema;
import org.jauntsy.grinder.karma.operations.map.Count;
import org.jauntsy.grinder.karma.operations.map.TopNMapper;
import org.jauntsy.grinder.karma.operations.reduce.MinValue;
import org.jauntsy.grinder.karma.operations.reduce.Sum;
import org.jauntsy.grinder.karma.operations.reduce.TopN;
import org.jauntsy.grinder.karma.operations.reduce.Union;
import org.jauntsy.grinder.karma.publish.Publisher;
import org.jauntsy.grinder.karma.testing.PrintBolt;
import org.jauntsy.grinder.replay.api.ReplayConfig;
import org.jauntsy.nice.Demo;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.jauntsy.nice.Nice.L;
import static org.jauntsy.nice.Nice.M;

/**
 * User: ebishop
 * Date: 1/2/13
 * Time: 9:50 AM
 */
public class _BankingDemo {

    public static void main(String[] args) throws IOException, ConnectionException {

        Logger.getRootLogger().setLevel(Level.WARN);

        // *** start the storm cluster
        LocalCluster cluster = new LocalCluster();

        // *** start kafka
        LocalKafkaBroker broker = new LocalKafkaBroker(0, 9090, 4, "localhost:2000");
        ReplayConfig replay = new ReplayConfig().staticHosts(broker.getHostPortStrings(), broker.getNumPartitions());

        // *** build a topology
//        KarmaConfig karmaConfig = new KarmaConfigImpl("a", replay, new InMemoryReducerState());
        KarmaConfig karmaConfig = new KarmaConfigImpl("a", replay, new CassandraReducerState("demo"));
        StormTopology topology = buildTopology(karmaConfig);

        // *** submit the topology to storm
        Config config = new Config();
        config.setMaxSpoutPending(50);
        cluster.submitTopology("bankdemo", config, topology);

        // *** send some events
        Producer<Long, Message> kafkaProducer = broker.buildSyncProducer();
        JsonProducer mb = new JsonProducer(kafkaProducer).printSendsToConsole(true);

        sendBankingDemoMessages(mb);

        Utils.sleep(100000);
        kafkaProducer.close();
    }

    private static StormTopology buildTopology(KarmaConfig karmaConfig) {

        KarmaTopologyBuilder builder = new KarmaTopologyBuilder(karmaConfig, "bankdemo");

        // *** setup all the source queues
        builder.setSpout("users", "users", new JsonScheme("id", "name"), 4);
        builder.setSpout("deposits", "deposits", new JsonScheme("id", "account", "amount"), 4);
        builder.setSpout("withdrawals", "withdrawals", new JsonScheme("id", "account", "amount"), 4);
        builder.setSpout("transfers", "transfers", new JsonScheme("id", "srcAccount", "dstAccount", "amount"), 4);

        // *** User Count: { count }
        // *** SELECT COUNT(*) as userCount FROM users WHERE name IS NOT NULL
        builder.map("{ name }", "users(id)", new Count()).red("{ count }", "userCount()", new Sum());
//        buildSniffer(builder, "userCount");

        /*

        Account Balance: { accountId balance }

        Equivalent to:
        SELECT account, SUM(balance) as balance FROM {
            SELECT account AS account, amount AS balance FROM deposits
            UNION ALL
            SELECT account AS account, -amount AS balance FROM withdrawals
            UNION ALL
            SELECT dstAccount AS account, amount AS balance FROM transfers
            UNION ALL
            SELECT srcAccount AS account, -amount AS balance FROM transfers
        } WHERE account IN NOT NULL AND balance IS NOT NULL GROUP BY account
         */
        builder.map("{ account amount }", "deposits(id)")
               .map("{ account amount }", "withdrawals(id)", "{ d -> emit(d.account, -d.amount) }")
               .map("{ dstAccount amount }", "transfers(id)")
               .map("{ srcAccount amount }", "transfers(id)", "{ d -> emit(d.srcAccount, -d.amount) }")
               .red("{ balance }", "accounts(account)", new Sum());

        /*

        Account Summary: { accountId, username, accountBalance }

        Equivalent to: TODO
         */
        builder.map("{ account balance }", "accounts(account)", "{ d -> emit(d.account, null, d.balance) }")
               .map("{ id name }", "users(id)", "{ u -> emit(u.id, u.name, null) }")
               .red("{ userName balance }", "accountJoin(id)", "{ a, b -> [a.userName ?: b.userName, a.balance ?: b.balance ] }");

        builder.map("{ id userName balance }", "accountJoin(id)")
               .red("{ userName balance }", "accountSummary(id)", new MinValue());
//        buildSniffer(builder, "accountSummary");

        /*
        Top Account: a temp table used for calculating top customer

        SELECT balance, account FROM {
            SELECT account, SUM(balance) as balance FROM {
                SELECT id AS account, amount AS balance FROM deposits
                UNION ALL
                SELECT id AS account, -amount AS balance FROM withdrawals
                UNION ALL
                SELECT dstAccount AS account, amount AS balance FROM transfers
                UNION ALL
                SELECT srcAccount AS account, -amount AS balance FROM transfers
            } WHERE account IS NOT NULL AND balance IS NOT NULL GROUP BY account
        } ORDER BY balance DESC, account DESC LIMIT 1
        */
//        karma.map("{ balance account }", "accountBalance(account)", "{ d -> emit([[d.balance, d.account]]) }")
        builder.map("{ balance account }", "accounts(account)", new TopNMapper())
               .red("{ topN }", "topAccounts()", new TopN(10)); // *** TopN expects a single column with a list of tuples in it

        /*
        Top Customer: { name }

        SELECT u.name as name FROM user u INNER JOIN {
            SELECT balance, account FROM {
                SELECT account as account, SUM(balance) as balance FROM {
                    SELECT id AS account, amount AS balance FROM deposits
                    UNION ALL
                    SELECT id AS account, -amount AS balance FROM withdrawals
                    UNION ALL
                    SELECT dstAccount AS account, amount AS balance FROM transfers
                    UNION ALL
                    SELECT srcAccount AS account, -amount AS balance FROM transfers
                } WHERE account IS NOT NULL AND balance IS NOT NULL GROUP BY account
            } ORDER BY balance DESC, account DESC LIMIT 1
        } j ON u.id = j.account
         */
        builder.map("{ topN }", "topAccounts()", "{ t -> t.topN.each { emit(it[1], null, it[0]) } }")
               .map("{ id name }", "users(id)", "{ u -> emit(u.id, u.name, null) }")
               .red("{ name balance }", "topCustomerStep1(account)", "{ a, b -> [a.name ?: b.name, a.balance ?: b.balance] }");

        builder.map("{ balance name account }", "topCustomerStep1(account)", new TopNMapper())
               .red("{ topN }", "topCustomers()", new TopN(10));
//        buildSniffer(builder, "topCustomers");

        builder.map("{ amount id }", "deposits(id)", new TopNMapper())
               .red("{ topN }", "topDeposits()", new TopN(10));

        /*

        Transaction Count: { accountId, transactions }

        Equivalent to:
        SELECT account as account, count(*) as transactions FROM {
            SELECT id as account, amount as amount FROM deposits
            UNION ALL
            SELECT id as account, amount as amount FROM withdrawals
            UNION ALL
            SELECT srcAccount as account, amount as amount FROM transfers
            UNION ALL
            SELECT dstAccount AS account, amount as amount FROM transfers
        } WHERE account IS NOT NULL AND amount IS NOT NULL GROUP BY account
         */
        builder.map("{ account amount }", "deposits(id) ", new Count("account"))
               .map("{ account amount }", "withdrawals(id) ", new Count("account"))
               .map("{ dstAccount amount }", "transfers(id) ", new Count("dstAccount"))
               .map("{ srcAccount amount }", "transfers(id) ", new Count("srcAccount"))
               .red("{ transactions }", "transactionCounts(account)", new Sum());
//        buildSniffer(builder, "transactionCounts");

        builder.map("{ account amount }", "deposits(id)", new Count())
               .map("{ account amount }", "withdrawals(id) ", new Count())
               .map("{ srcAccount dstAccount amount }", "transfers(id) ", new Count())
               .red("{ transactions }", "totalTransactions()", new Sum());
//        buildSniffer(builder, "totalTransactions");

        /*
        Summary: { users transactions totalAssets topCustomer }

        SELECT SUM(amount) FROM {
            SELECT COUNT(*) as users FROM users WHERE name IS NOT NULL
            SELECT COUNT(*) as accounts FROM accounts WHERE balance IS NOT NULL
            SELECT SUM(counts) as transactions FROM {
                SELECT COUNT(*) as counts FROM deposits WHERE amount IS NOT NULL
                UNION ALL
                SELECT COUNT(*) as counts FROM withdrawals WHERE amount IS NOT NULL
                UNION ALL
                SELECT COUNT(*) as counts FROM transfers WHERE amount IS NOT NULL
            }
            SELECT SUM(balance) as totalAssets FROM {
                SELECT SUM(amount) as balance FROM deposits WHERE amount IS NOT NUlL
                UNION ALL
                SELECT SUM(-amount) as balance FROM withdrawals WHERE amount IS NOT NULL
            } WHERE balance IN NOT NULL
        } WHERE ..., ..., ... TODO: finish this
         */
        builder.map("{ amount }", "deposits(id)", "{ d -> emit(0, 1, d.amount, null) }")
               .map("{ amount }", "withdrawals(id)", "{ d -> emit(0, 1, -d.amount, null) }")
               .map("{ srcAccount dstAccount }", "transfers(id)", "{ d -> emit(0, 1, 0, null) }")
               .map("{ name }", "users(id)", "{ d -> emit(1, 0, 0, null) }")
               .map("{ topN }", "topCustomers()", "{ d -> emit(0, 0, 0, d.topN) }")
               .red("{ users transactions totalAssets topCustomers }", "summary()", new Union(new Sum(), new Sum(), new Sum(), new MinValue()));
//        buildSniffer(builder, "summary");

        String duckStream = "default";
        builder.setBolt("duckAssets", new DucksBoardCounterBolt(new Fields(), "115222", "totalAssets")).globalGrouping("summary", duckStream);
        builder.setBolt("duckTransactions", new DucksBoardCounterBolt(new Fields(), "115268", "transactions")).globalGrouping("totalTransactions", duckStream);
        builder.setBolt("duckTopCustomers", new DucksBoardLeaderboardBolt(new Fields(), "115264", "topN", 1, 0)).globalGrouping("topCustomers", duckStream);
        builder.setBolt("duckTopDeposits", new DucksBoardLeaderboardBolt(new Fields(), "116744", "topN", 1, 0)).globalGrouping("topDeposits", duckStream);

        builder.map("{ amount }", "deposits(id)", new Count()).red("{ value }", "depositCount()", new Sum());
        builder.map("{ amount }", "withdrawals(id)", new Count()).red("{ value }", "withdrawalCount()", new Sum());
        builder.map("{ amount }", "transfers(id)", new Count()).red("{ value }", "transferCount()", new Sum());
        builder.setBolt("duckDeposits", new DucksBoardCounterBolt(new Fields(), "116794", "value")).globalGrouping("depositCount", duckStream);
        builder.setBolt("duckWithdrawals", new DucksBoardCounterBolt(new Fields(), "116795", "value")).globalGrouping("withdrawalCount", duckStream);
        builder.setBolt("duckTransfers", new DucksBoardCounterBolt(new Fields(), "116796", "value")).globalGrouping("transferCount", duckStream);


        List<String> tables = L("users(id)", "userCount()", "accounts(account)", "topCustomers()", "transactionCounts(account)", "totalTransactions()", "summary()", "transfers(id)");
        publishDb(builder, "cass", tables, new CassandraPublisher("bank"));
        publishDb(builder, "cloud", tables, new CouchDbPublisher("https://eldenbishop.cloudant.com", "eldenbishop", "shinobu", "bank"));

        return builder.createTopology();
    }

    private static void publishDb(KarmaTopologyBuilder builder, String id, List<String> tables, Publisher factory) {
        for (String schema : tables) {
            Schema s = Schema.parse(schema);
            String table = s.getName();
            Fields idFields = new Fields(s.getIdFields());
            builder.setBolt(id + "_" + table, factory.newBolt(table, idFields)).fieldsGrouping(table, idFields);
        }
    }

    private static void sendBankingDemoMessages(JsonProducer mb) {

        Utils.sleep(5000);

//        Demo.readEnter("\n\nCreating Joe", 1);
        mb.update("users(id)", M("id", "a", "name", "Joe"));

//        Demo.readEnter("\n\nCreating Sue", 1);
        mb.update("users(id)", M("id", "b", "name", "Sue"));

        Demo.readEnter("\n\nDeposit 100$ to Joe, account A", 1);
        mb.update("deposits(id)", M("id", 1, "account", "a", "amount", 100.0));

        Demo.readEnter("\n\nDeposit 50$ to Sue, account B", 1);
        mb.update("deposits(id)", M("id", 2, "account", "b", "amount", 50.0));

        Demo.readEnter("\n\nWithdraw 10$ from Joe, account A", 1);
        mb.update("withdrawals(id)", M("id", 1, "account", "a", "amount", 10.0));

        Demo.readEnter("\n\nTransfer 30$ from Joe, account A to Sue, account B", 1);
        mb.update("transfers(id)", M("id", 1, "srcAccount", "a", "dstAccount", "b", "amount", 30.0));

        Demo.readEnter("\n\nInvalidating previous transfer", 1);
        mb.update("transfers(id)", M("id", 1, "srcAccount", null, "dstAccount", null, "amount", null));

        int lotterySize = 1000;
        Demo.readEnter("\n\nLottery win", 1);
        Random random = new Random(0L);
        for (int i = 0; i < lotterySize; i++) {
            mb.update("deposits(id)", M("id", 3 + i, "account", "a", "amount", 10.0 + rando(random, 100000)));
        }

        Demo.readEnter("\n\nLottery lose", 1);
        for (int i = 0; i < lotterySize; i++) {
            mb.update("deposits(id)", M("id", 3 + i, "account", null, "amount", null));
        }

        Demo.readEnter("\n\nDeleting everything else", 1);
        mb.update("users(id)", M("id", "a", "name", null));
        mb.update("users(id)", M("id", "b", "name", null));
        mb.update("withdrawals(id)", M("id", 1, "account", null, "amount", null));
        mb.update("deposits(id)", M("id", 1, "account", null, "amount", null));
        mb.update("deposits(id)", M("id", 2, "account", null, "amount", null));
    }







    private static void buildSniffer(KarmaTopologyBuilder builder, String srcComponentId) {
        buildSniffer(builder, srcComponentId, "changes");
    }

    private static void buildSniffer(KarmaTopologyBuilder builder, String srcComponentId, String stream) {
        builder.setBolt(srcComponentId + "Sniffer", new PrintBolt("### " + srcComponentId, 1000, false)).globalGrouping(srcComponentId, stream);
    }

    private static int rando(Random random, int range) {
        double x = random.nextDouble();
        double y = x * x * x * x * x * x * x * x * x * x * x * x * x * x * x * x * x * x * x * x * x * x;
        return (int)(y * range);
    }

}
