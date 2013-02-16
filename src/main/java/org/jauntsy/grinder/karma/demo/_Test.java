package org.jauntsy.grinder.karma.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.karma.Emitter;
import org.jauntsy.grinder.karma.KarmaTopologyBuilder;
import org.jauntsy.grinder.karma.KarmaConfig;
import org.jauntsy.grinder.karma.mapred.Crc64;
import org.jauntsy.grinder.karma.mapred.Mapper;
import org.jauntsy.grinder.karma.mapred.Reducer;
import org.jauntsy.grinder.karma.operations.map.Count;
import org.jauntsy.grinder.karma.operations.reduce.Sum;
import org.jauntsy.grinder.karma.serialization.DefaultSerializer;
import org.jauntsy.grinder.karma.serialization.TupleSerializer;
import org.jauntsy.grinder.karma.testing.InMemoryReducerState;
import org.jauntsy.grinder.karma.testing.PrintBolt;
import org.jauntsy.grinder.replay.api.ReplayConfig;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaSpout;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import static org.jauntsy.nice.Nice.L;

/**
 * User: ebishop
 * Date: 1/2/13
 * Time: 9:50 AM
 */
public class _Test {

    private static final String[] orgNames = { "Taco Bell", "Kfc", "Pizza Hut" };
    private static final int[] orgAssignment = { 0, 1, 1, 2, 2, 2 };

    private static final String[] names = { "Joe", "Sue", "Bob", "Ted", "Sam", "Pam", "Tod", "Larry", "Kim", "Kip" };

    public static final TupleScheme ORG_SCHEME = new TupleScheme(new Fields("id", "name"));
    public static final TupleScheme USER_SCHEME = new TupleScheme(new Fields("id", "name", "orgId"));

    public static void main(String[] args) throws IOException {

        Logger.getRootLogger().setLevel(Level.WARN);

        // *** start the storm cluster
        LocalCluster cluster = new LocalCluster();

        // *** start the embedded kafka service
        LocalKafkaBroker broker = new LocalKafkaBroker(0, 9090, 1, "localhost:2000");

        // *** configure replay and karma to use the local kafka instance
        ReplayConfig replay = new ReplayConfig().staticHosts(broker.getHostPortStrings(), broker.getNumPartitions());
        KarmaConfig karmaConfig = new KarmaConfig("a").replay(replay).reducerState(new InMemoryReducerState());

        KarmaTopologyBuilder karma = new KarmaTopologyBuilder(karmaConfig, "testA");

        karma.setSpout("orgSpout", new KafkaSpout(replay.buildReplaySpoutConfig("org", ORG_SCHEME, "orgSpoutId")), 4);
        karma.setSpout("userSpout", new KafkaSpout(replay.buildReplaySpoutConfig("user", USER_SCHEME, "userSpoutId")), 4);

        karma.map("{ orgId }", "userSpout(id)", new Count("orgId"))
             .red("{ userCount }", "orgUserCounts(orgId)", new Sum());
        karma.map("{ userCount }", "orgUserCounts(orgId)", new Count("userCount"))
             .red("{ total samples }", "allOrgs()", new Sum())
             .fmt("{ totalUsers averagePerOrg }", "{ d -> [d.total, d.total / d.samples] }");
        buildSniffer("allOrgs", karma);

        karma.map("{ orgId }", "userSpout(id)", "{ u -> emit(u.orgId, 1) }")
             .red("{ userCount }", "orgUserCounts2(orgId)", "{ a, b -> [a.userCount + b.userCount] }");
        karma.map("{ userCount }", "orgUserCounts2(orgId)", "{ d -> emit(d.userCount, 1) }")
             .red("{ total samples }", "allOrgs2()", "{ a, b -> [a.total + b.total, a.samples + b.samples]}")
             .fmt("{ totalUsers averagePerOrg }", "{ d -> [d.total, d.total / d.samples] }");
        buildSniffer("allOrgs2", karma);

        // *** build a name count using the scripting support
        karma.map("{ name }", "userSpout(id)", "{ u -> emit(u.name, 1L) }")
             .red("{ count }", "nameCounts(name)", "{ a, b -> [a.count + b.count] }");
        buildSniffer("nameCounts", karma);

        karma.map("{ name }", "userSpout(id)", "{ u -> emit(u.name, 1L) }")
             .red("{ count }", "nameCounts2(name)", "{ a, b -> [a.count + b.count] }");
        buildSniffer("nameCounts2", karma);

        karma.map("{ orgId }", "userSpout(id)", "{ u -> emit(u.orgId, 1L) }")
             .red("{ count }", "empCounts(orgId)", "{ a, b -> [a.count + b.count] }");
        buildSniffer("empCounts", karma);

        karma.map("{ name }", "userSpout(id)", "{ u -> emit(1L) }")
             .red("{ count }", "userCount()", "{ a, b -> [a.count + b.count] }");
        buildSniffer("userCount", karma);

        karma.map("{ name }", "userSpout(id)", "{ u -> emit(1L) }")
             .red("{ count }", "userCount2()", "{ a, b -> [a.count + b.count] }");
        buildSniffer("userCount2", karma);

        karma
            .map("{ id name }", "orgSpout(id)", new Mapper() {
                @Override
                public void map(Tuple t, Emitter e) {
                    e.emit(t.getValueByField("id"), t.getStringByField("name"), L());
                }
            })
            .map("{ orgId name }", "userSpout(id)", new Mapper() {
                @Override
                public void map(Tuple t, Emitter e) {
                    e.emit(t.getValueByField("orgId"), null, L(t.getStringByField("name")));
                }
            })
            .red("{ orgName userNames }", "orgToUsernames(orgId)", new Reducer() {
                @Override
                public List reduce(Tuple key, Tuple a, Tuple b) {
                    Set<String> names = new TreeSet<String>();
                    names.addAll((List) (a.getValueByField("userNames")));
                    names.addAll((List) (b.getValueByField("userNames")));
                    return L(a.getString(0) != null ? a.getString(0) : b.getString(0), new ArrayList(names));
                }
            });

        karma.map("orgSpout", L("id"), L("id", "name"), new Mapper() {
            @Override
            public void map(Tuple t, Emitter e) {
                e.emit(t.getValueByField("id"), t.getStringByField("name"), L());
            }
        }).map("userSpout", L("id"), L("orgId", "name"), new Mapper() {
            @Override
            public void map(Tuple t, Emitter e) {
                e.emit(t.getValueByField("orgId"), null, L(t.getStringByField("name")));
            }
        }).red("{ orgName userNames }", "org2Usernames(orgId)", new Reducer() {
            @Override
            public List reduce(Tuple key, Tuple a, Tuple b) {
                Set<String> names = new TreeSet<String>();
                names.addAll((List) (a.getValueByField("userNames")));
                names.addAll((List) (b.getValueByField("userNames")));
                return L(a.getString(0) != null ? a.getString(0) : b.getString(0), new ArrayList(names));
            }
        });
        buildSniffer("org2Usernames", karma);


        karma.map("{ orgName userNames }", "org2Usernames(orgId)", new Mapper() {
            @Override
            public void map(Tuple t, Emitter e) {
                String orgName = t.getStringByField("orgName");
                if (orgName != null)
                    for (String userName : (List<String>) t.getValueByField("userNames")) {
                        e.emit(userName, L(orgName));
                    }
            }
        }).red("{ orgNames }", "userNames2OrgNames(userName)", new Reducer() {
            @Override
            public List reduce(Tuple key, Tuple a, Tuple b) {
                System.out.println("userNames2OrgNames reducing: a: " + a + ", b: " + b);
                Set<String> orgNames = new TreeSet<String>();
                orgNames.addAll((List) a.getValue(0));
                orgNames.addAll((List) b.getValue(0));
                return L(new ArrayList(orgNames));
            }
        });

        buildSniffer("userNames2OrgNames", karma);

        cluster.submitTopology("karma", new Config(), karma.createTopology());

        Producer<Long,Message> producer = broker.buildSyncProducer();

//        Demo.countdown("Adding orgs and users in ", 5);

//        sendOrgs(producer);
//        send100Users(producer);

//        Demo.readEnter("*** Adding acme", 3);
        sendOrg(producer, 1000, "Acme");

//        Demo.readEnter("*** Adding 10 greggs", 3);
        for (int i = 0; i < 10; i++) {
//            Demo.readEnter("** Adding gregg " + (i + 1), 1);
            sendUser(producer, 2000 + i, "Gregg", 1000);
        }

//        Demo.readEnter("*** Changing greggs to seth and assigning to org 1 in", 3);
        Utils.sleep(2000);
        sendOrg(producer, 1, "Kfc");
        for (int i = 0; i < 10; i++) {
//            Demo.readEnter("** Changing gregg " + (i + 1) + " to seth and Kfc in", 1);
            sendUser(producer, 2000 + i, "Seth", 1);
        }

//        Demo.readEnter("*** Deleting acme", 3);
        for (int i = 0; i < 10; i++)
            deleteUser(producer, 2000 + i);
        deleteOrg(producer, 1000);
        deleteOrg(producer, 1);

        Utils.sleep(100000);

        producer.close();
    }

    private static void buildSniffer(String srcComponentId, KarmaTopologyBuilder builder) {
        builder.setBolt(srcComponentId + "Sniffer", new PrintBolt("### " +
                srcComponentId)).globalGrouping(srcComponentId, "changes");
    }

    private static void sendOrgs(Producer<Long, Message> producer) throws UnsupportedEncodingException {
        for (int i = 0; i < orgNames.length; i++) {
            send(producer, "org", L(i), L(orgNames[i]));
        }
    }

    private static void sendOrg(Producer<Long, Message> producer, int id, String name) {
        send(producer, "org", L(id), L(name));
    }

    private static void deleteOrg(Producer<Long, Message> producer, int id) {
        send(producer, "org", L(id), L((String)null));
    }

    private static void send100Users(Producer<Long,Message> producer) throws UnsupportedEncodingException {
        if (true) {
            Stack<Integer> kips = new Stack<Integer>();
            for (int i = 0; i < 10; i++) {
                for (int j = i; j < 10; j++) {
                    int id = i * 10 + j;
                    String name = names[id % names.length];
                    int orgId = orgAssignment[id % orgAssignment.length];
//                    int orgId = orgAssignment[(int)(Math.random() * orgAssignment.length)];
                    sendUser(producer, id, name, orgId);
                    if ("Kip".equals(name))
                        kips.add(id);
                }
            }
//            Demo.countdown("Rename all kips to michael and assigning org to Taco Bell in:", 5);
            while (kips.size() > 0) {
                int id = kips.pop();
//                System.out.println("Renaming user " + id);
                sendUser(producer, id, "Michael", 0);
//                Utils.sleep(1000);
            }
        }
    }

    private static TupleSerializer ser = new DefaultSerializer();

    private static void sendUser(Producer<Long, Message> producer, long id, String name, int orgId) throws UnsupportedEncodingException {
        send(producer, "user", L(id), L(name, orgId));
    }

    private static void deleteUser(Producer<Long, Message> producer, long id) throws UnsupportedEncodingException {
        send(producer, "user", L(id), L(null, null));
    }

    private static void send(Producer<Long, Message> producer, String topic, List id, List value) {
        List tuple = new ArrayList();
        tuple.addAll(id);
        tuple.addAll(value);
        byte[] bytes = ser.serialize(tuple);
        producer.send(new ProducerData<Long,Message>(
                topic,
                Crc64.getCrc(JSONValue.toJSONString(id).getBytes()),
                L(new Message(bytes))
        ));
    }

    public static class TupleScheme implements Scheme {

        private final Fields fields;

        private transient TupleSerializer ser;

        public TupleScheme(Fields fields) {
            this.fields = fields;
        }

        @Override
        public List<Object> deserialize(byte[] bytes) {
            if (ser == null)
                ser = new DefaultSerializer();
            return ser.deserialize(bytes);
        }

        @Override
        public Fields getOutputFields() {
            return this.fields;
        }
    }

}
