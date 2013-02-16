package org.jauntsy.grinder.karma.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.karma.KarmaTopologyBuilder;
import org.jauntsy.grinder.karma.KarmaConfig;
import org.jauntsy.grinder.karma.KarmaConfigImpl;
import org.jauntsy.grinder.karma.mapred.ReducerState;
import org.jauntsy.grinder.karma.operations.reduce.MinValue;
import org.jauntsy.grinder.karma.operations.reduce.Union;
import org.jauntsy.grinder.karma.testing.InMemoryReducerState;
import org.jauntsy.grinder.karma.testing.PrintBolt;
import org.jauntsy.grinder.replay.api.ReplayConfig;
import org.jauntsy.grinder.replay.api.ReplaySpoutConfig;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaSpout;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

import static org.jauntsy.nice.Nice.M;

/**
 * User: ebishop
 * Date: 1/2/13
 * Time: 9:50 AM
 */
public class _ResponseTimeTest {

    public static void main(String[] args) throws IOException {

        Logger.getRootLogger().setLevel(Level.WARN);

        // *** start the storm cluster
        LocalCluster cluster = new LocalCluster();

        // *** start the embedded kafka service
        LocalKafkaBroker broker = new LocalKafkaBroker(0, 9090, 4, "localhost:2000");

        // *** configure replay and karma to use the local kafka instance
        ReplayConfig replay = new ReplayConfig().staticHosts(broker.getHostPortStrings(), broker.getNumPartitions());

        KarmaConfig karmaConfig = new KarmaConfigImpl("a", replay, prepReducerState("a"));
//        KarmaConfig karmaConfig = new LocalKarmaConfig("a", cluster, broker, new HBaseReducerState());

        KarmaTopologyBuilder karma = new KarmaTopologyBuilder(karmaConfig, "rtt");

        karma.setSpout("updates", new KafkaSpout(new ReplaySpoutConfig(replay, "updates", new JsonScheme("channelId", "tweetId", "inResponseTo", "ts"), "updateSpoutId")), 4);

        // *** emits: channelId, tweetId => responseTime
        karma.map("{ channelId tweetId ts }", "updates(channelId tweetId)", "{ d -> emit(d.channelId, d.tweetId, d.ts, null) }")
             .map("{ channelId tweetId inResponseTo ts }", "updates(channelId tweetId)", "{ d -> emit(d.channelId, d.inResponseTo, null, d.ts) }")
             .red("{ tweetTs respTs }", "responseTime(channelId tweetId)", new Union(new MinValue(), new MinValue()))
             .fmt("{ responseTime }", "{ d -> [d.respTs - d.tweetTs] }");
        buildSniffer(karma, "responseTime");
//

        cluster.submitTopology("mytopo", new Config(), karma.createTopology());

        Producer<Long, Message> producer = broker.buildSyncProducer();
        JsonProducer mb = new JsonProducer(producer).printSendsToConsole(true);

        transferDemo(mb);

        Utils.sleep(100000);
        producer.close();
    }

    private static void transferDemo(JsonProducer mb) {

        mb.update("updates(channelId tweetId)", M("channelId", 7, "tweetId", 1001, "ts", 1L)); Utils.sleep(250);
        mb.update("updates(channelId tweetId)", M("channelId", 7, "tweetId", 1002, "ts", 2L)); Utils.sleep(250);
        mb.update("updates(channelId tweetId)", M("channelId", 7, "tweetId", 1003, "inResponseTo", 1001, "ts", 3L)); Utils.sleep(250);
        mb.update("updates(channelId tweetId)", M("channelId", 7, "tweetId", 1004, "inResponseTo", 1001, "ts", 4L)); Utils.sleep(250);

        Utils.sleep(4000);
        System.out.println("\n\nDeleting first response");
        mb.update("updates(channelId tweetId)", M("channelId", 7, "tweetId", 1003, "inResponseTo", null, "ts", null));

    }







    private static ReducerState prepReducerState(String ns) throws IOException {
        return new InMemoryReducerState();
    }

    private static void buildSniffer(KarmaTopologyBuilder builder, String srcComponentId) {
        buildSniffer(builder, srcComponentId, "changes");
    }

    private static void buildSniffer(KarmaTopologyBuilder builder, String srcComponentId, String stream) {
        builder.setBolt(srcComponentId + "Sniffer", new PrintBolt("### " + srcComponentId, 0, false)).globalGrouping(srcComponentId, stream);
    }


}
