package org.jauntsy.grinder.karma.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.karma.Emitter;
import org.jauntsy.grinder.karma.KarmaTopologyBuilder;
import org.jauntsy.grinder.karma.KarmaConfig;
import org.jauntsy.grinder.karma.mapred.*;
import org.jauntsy.grinder.karma.mapred.Formatter;
import org.jauntsy.grinder.karma.operations.map.Count;
import org.jauntsy.grinder.karma.operations.reduce.Sum;
import org.jauntsy.grinder.karma.operations.reduce.TopN;
import org.jauntsy.grinder.karma.operations.map.TopNMapper;
import org.jauntsy.grinder.karma.testing.InMemoryReducerState;
import org.jauntsy.grinder.karma.testing.PrintBolt;
import org.jauntsy.grinder.panama.UniversalComparator;
import org.jauntsy.grinder.replay.api.ReplayConfig;
import org.jauntsy.grinder.replay.api.ReplaySpoutConfig;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaSpout;
import org.jauntsy.nice.Demo;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import static org.jauntsy.nice.Nice.*;

/**
 * User: ebishop
 * Date: 1/2/13
 * Time: 9:50 AM
 */
public class _Demo {

    private static final boolean DROP_MONGO = true;
    private static final String DB_NAME = "karma_example";

    private static final String[] orgNames = { "Taco Bell", "Kfc", "Pizza Hut" };
    private static final int[] orgAssignment = { 0, 1, 1, 2, 2, 2 };

    private static final String[] names = { "Joe", "Sue", "Bob", "Ted", "Sam", "Pam", "Tod", "Larry", "Kim", "Kip" };

    public static void main(String[] args) throws IOException {

        Logger.getRootLogger().setLevel(Level.WARN);

//        if (DROP_MONGO) {
//            Mongo mongo = new Mongo("localhost");
//            mongo.getDB(DB_NAME).dropDatabase();
//            mongo.close();
//        }

        // *** start the storm cluster
        LocalCluster cluster = new LocalCluster();

        // *** start the embedded kafka service
        LocalKafkaBroker broker = new LocalKafkaBroker(0, 9090, 4, "localhost:2000");

        // *** configure replay and karma to use the local kafka instance
        ReplayConfig replay = new ReplayConfig().staticHosts(broker.getHostPortStrings(), broker.getNumPartitions());
        KarmaConfig karmaConfig = new KarmaConfig("a").replay(replay).reducerState(new InMemoryReducerState());


        KarmaTopologyBuilder karma = new KarmaTopologyBuilder(karmaConfig, "demoA");

        karma.setSpout("orgSpout", new KafkaSpout(new ReplaySpoutConfig(replay, "org", JsonProducer.ORG_SCHEME, "orgSpoutId")), 4);
        karma.setSpout("userSpout", new KafkaSpout(new ReplaySpoutConfig(replay, "user", JsonProducer.USER_SCHEME, "userSpoutId")), 4);

        // *** build an org count
        karma.map("{ name }", "orgSpout(id)", new Mapper() {
            @Override
            public void map(Tuple t, Emitter e) {
                e.emit(1);
            }
        }).red("{ count }", "orgCounts()", new Reducer() {
            @Override
            public List reduce(Tuple key, Tuple a, Tuple b) {
                return L(a.getLong(0) + b.getLong(0));
            }
        });
        buildSniffer("orgCounts", karma);

        karma.map("{ name }", "userSpout(id)", new Count()).red("{ count }", "userCount()", new Sum());


        // *** build a user count using built-in operations
        karma.map("{ name }", "userSpout(id)", new Count()).red("{ count }", "userCounts()", new Sum());
        buildSniffer("userCounts", karma);

        // *** build a name count using the scripting support
        karma.map("{ name }", "userSpout(id)", "{ u -> emit(u.name, 1L) }")
             .red("{ count }", "nameCounts(name)", "{ a, b -> [a.count + b.count] }");
        buildSniffer("nameCounts", karma);


        karma.map("{ count name }", "nameCounts(name)", new Mapper() {
            @Override
            public void map(Tuple t, Emitter e) {
                e.emit(L(L(t.getLong(0), t.getString(1)))); // *** uh oh, confused yet? tuple contains a list of tuples (also a list) containing name and count
            }
        }).red("{ top4 }", "top4Names()", new Reducer() {
            @Override
            public List reduce(Tuple key, Tuple a, Tuple b) {
                // *** get the two lists of tuples, put them all in a treeset which sorts them by columns
                TreeSet set = addall(new TreeSet(UniversalComparator.LIST_COMPARATOR), (List) a.getValue(0), (List) b.getValue(0));
                // *** trim the fat
                while (set.size() > 4)
                    set.remove(set.first());
                return L(new ArrayList(set));
            }
        });
        buildSniffer("top4Names", karma);

        karma.map("{ count name }", "nameCounts(name)", new TopNMapper()).red("{ top4 }", "top4Names2()", new TopN(4));
        buildSniffer("top4Names2", karma);

        // *** Join orgs and users, note that this takes TWO spouts as input, each with a custom map
//        karma.mapTo("orgId", "orgName", "empCount")
//                .from("orgSpout(id) { id name }").withMapper("{ o -> emit(o.id, o.name, 0L) }")
//                .from("userSpout(id) { orgId }").withMapper("{ u -> emit(u.orgId, null, 1L) }")
//             .merge("orgName", "empCount")
//                .withReducer("{ a, b -> [a[0] ?: b[0], a[1] + b[1]] }")
//             .newStream("orgEmployeeCounts");
//        buildSniffer("orgEmployeeCounts", builder);

        karma.map("{ id name }", "orgSpout(id) ", "{ o -> emit(o.id, o.name, 0L) }")
             .map("{ orgId }", "userSpout(id)", "{ u -> emit(u.orgId, null, 1L) }")
             .red("{ orgName empCount }", "orgEmployeeCounts(orgId)", "{ a, b -> [a[0] ?: b[0], a[1] + b[1]] }");

        buildSniffer("orgEmployeeCounts", karma);


        karma
                .map("{ empCount }", "orgEmployeeCounts(orgId)", new Mapper() {
                    @Override
                    public void map(Tuple t, Emitter e) {
                        e.emit(t.getLong(0), 1L);
                    }
                })
                .red("{ count samples }", "avgNumEmployees()", new Sum())
                .fmt("{ avg }", new Formatter() {
                    @Override
                    public List merge(Tuple t) {
                        return L(t.getLong(0) / t.getLong(1));
                    }
                });
        buildSniffer("avgNumEmployees", karma);

        karma.map("{ id name }", "orgSpout(id)", new Mapper() {
            @Override
            public void map(Tuple t, Emitter e) {
                e.emit(t.getValueByField("id"), t.getStringByField("name"), L());
            }
        }).map("{ orgId name }", "userSpout(id)", new Mapper() {
            @Override
            public void map(Tuple t, Emitter e) {
                e.emit(t.getValueByField("orgId"), null, L(t.getStringByField("name")));
            }
        }).red("{ orgName userNames }", "org2Usernames(orgId)", new Reducer() {
            @Override
            public List reduce(Tuple key, Tuple a, Tuple b) {
                Set<String> userNames = addall(new TreeSet<String>(), (List) (a.getValueByField("userNames")), (List) (b.getValueByField("userNames")));
                return L(a.getString(0) != null ? a.getString(0) : b.getString(0), new ArrayList(userNames));
            }
        });
        buildSniffer("org2Usernames", karma);

//        builder.setBolt("asdf", new PrintBolt("org2Usernames")).globalGrouping("org2Usernames", "changes");

        karma.map("org2Usernames", L("orgId"), L("orgName", "userNames"), new Mapper() {
            @Override
            public void map(Tuple t, Emitter e) {
                String orgName = t.getStringByField("orgName");
                for (String userName : (List<String>) t.getValueByField("userNames")) {
                    e.emit(userName, L(orgName));
                }
            }
        }).red("{ orgNames }", "userNames2OrgNames(userName)", new Reducer() {
            @Override
            public List reduce(Tuple key, Tuple a, Tuple b) {
                Set<String> orgNames = addall(new TreeSet<String>(), (List) a.getValue(0), (List) b.getValue(0));
                return L(new ArrayList(orgNames));
            }
        });
        buildSniffer("userNames2OrgNames", karma);
//        builder.setBolt("qwer", new PrintBolt("userNames2OrgNames")).globalGrouping("userNames2OrgNames", "changes");

        karma
                .map("{ top4 }", "top4Names()", new Mapper() {
                    @Override
                    public void map(Tuple t, Emitter e) {
                        for (List l : (List<List>) t.getValue(0)) {
                            e.emit(l.get(1), L(), true);
                        }
                    }
                })
                .map("{ userName orgNames }", "userNames2OrgNames(userName)", new Mapper() {
                    @Override
                    public void map(Tuple t, Emitter e) {
                        e.emit(t.getStringByField("userName"), t.getValueByField("orgNames"), false);
                    }
                })
                .red("{ orgNames isTop4 }", "usernamesToOrgnamesWTop4bit(userName)", new Reducer() {
                    @Override
                    public List reduce(Tuple key, Tuple a, Tuple b) {
                        return L(
                                new ArrayList(addall(new TreeSet<String>(), (List) a.getValue(0), (List) b.getValue(0))),
                                a.getBoolean(1) | b.getBoolean(1)
                        );
                    }
                });
//        buildSniffer("usernamesToOrgnamesWTop4bit", builder);

        karma.map("{ orgNames isTop4 }", "usernamesToOrgnamesWTop4bit(userName)", new Mapper() {
            @Override
            public void map(Tuple t, Emitter e) {
                if (t.getBoolean(1))
                    e.emit(t.getValue(0));
            }
        }).red("{ orgNames }", "orgsWithUsernamesInTop4()", new Reducer() {
            @Override
            public List reduce(Tuple key, Tuple a, Tuple b) {
                Set<String> orgNames = addall(new TreeSet<String>(), (List) a.getValue(0), (List) b.getValue(0));
                return list(flatlist(orgNames));
            }
        });
        buildSniffer("orgsWithUsernamesInTop4", karma);

        Config config = new Config();
        config.setFallBackOnJavaSerialization(false);
        cluster.submitTopology("karma", config, karma.createTopology());

        Producer<Long,Message> producer = broker.buildSyncProducer();
        JsonProducer mb = new JsonProducer(producer);

        Demo.countdown("Adding orgs and users in ", 5);

        sendOrgs(mb);
        send100Users(mb);

        Demo.readEnter("*** Adding acme", 3);
        mb.sendTuple("org", 1, L(1000, "Acme"));

        Utils.sleep(1000);
        System.out.println("*** Adding 10 greggs");
        for (int i = 0; i < 10; i++) {
            Demo.readEnter("** Adding gregg " + (i + 1), 1);
            mb.sendTuple("user", 1, L(2000 + i, "Gregg", 1000));
        }

        Demo.readEnter("*** Changing greggs to seth and assigning to org 1 in", 3);
        for (int i = 0; i < 10; i++) {
            Demo.readEnter("** Changing gregg " + (i + 1) + " to seth and Kfc in", 1);
            mb.sendTuple("user", 1, L(2000 + i, "Seth", 1));
        }

        Demo.readEnter("*** Deleting acme", 3);
        mb.sendTuple("org", 1, L(1000, null));

        Utils.sleep(100000);

        producer.close();
    }

    private static void buildSniffer(String srcComponentId, KarmaTopologyBuilder builder) {
        builder.setBolt(srcComponentId + "Sniffer", new PrintBolt(srcComponentId)).globalGrouping(srcComponentId, "changes");
    }

    private static void sendOrgs(JsonProducer mb) throws UnsupportedEncodingException {
        for (int i = 0; i < orgNames.length; i++) {
            mb.sendTuple("org", 1, L(i, orgNames[i]));
        }
    }

    private static void send100Users(JsonProducer mb) throws UnsupportedEncodingException {
        if (true) {
            Stack<Integer> kips = new Stack<Integer>();
            for (int i = 0; i < 10; i++) {
                for (int j = i; j < 10; j++) {
                    int id = i * 10 + j;
                    String name = names[id % names.length];
                    int orgId = orgAssignment[id % orgAssignment.length];
//                    int orgId = orgAssignment[(int)(Math.random() * orgAssignment.length)];
                    mb.sendUser(id, name, orgId);
                    if ("Kip".equals(name))
                        kips.add(id);
                }
            }
            Demo.countdown("Rename all kips to michael and assigning org to Taco Bell in:", 5);
            while (kips.size() > 0) {
                int id = kips.pop();
//                System.out.println("Renaming user " + id);
                mb.sendUser(id, "Michael", 0);
//                Utils.sleep(1000);
            }
        }
    }

}
