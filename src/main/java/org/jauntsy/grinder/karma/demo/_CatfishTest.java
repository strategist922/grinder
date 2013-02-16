package org.jauntsy.grinder.karma.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;
import com.exacttarget.spike.KafkaUtil;
import org.jauntsy.grinder.karma.KarmaTopologyBuilder;
import org.jauntsy.grinder.karma.KarmaConfig;
import org.jauntsy.grinder.karma.KarmaConfigImpl;
import org.jauntsy.grinder.karma.contrib.cassandra.CassandraReducerState;
import org.jauntsy.grinder.karma.mapred.ReducerState;
import org.jauntsy.grinder.karma.operations.map.Count;
import org.jauntsy.grinder.karma.operations.map.TopNMapper;
import org.jauntsy.grinder.karma.operations.reduce.Sum;
import org.jauntsy.grinder.karma.operations.reduce.TopN;
import org.jauntsy.grinder.karma.testing.InMemoryReducerState;
import org.jauntsy.grinder.karma.testing.PrintBolt;
import org.jauntsy.grinder.replay.api.ReplayConfig;
import org.jauntsy.grinder.replay.api.ReplaySpoutConfig;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaSpout;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.jauntsy.nice.Nice.L;
import static org.jauntsy.nice.Nice.M;

/**
 * User: ebishop
 * Date: 1/2/13
 * Time: 9:50 AM
 */
public class _CatfishTest {

    public static void main(String[] args) throws IOException {

        Charset UTF8 = Charset.forName("UTF-8");
        Logger.getRootLogger().setLevel(Level.WARN);

        SimpleConsumer consumer = new SimpleConsumer("127.0.0.1", 9092, 10000, 100000);
//        SimpleConsumer consumer = new SimpleConsumer("cfkafka01.stg.cotweet.com", 2181, 100000, 100000);
        if (false) {
            long offset = KafkaUtil.getOffsetBeforeEarliestTime(consumer, "tweets", 0);
            while (true) {
                ByteBufferMessageSet messageAndOffsets = consumer.fetch(new FetchRequest("tweets", 0, offset, 100000));
                for (MessageAndOffset mno : messageAndOffsets) {
                    byte[] bytes = KafkaUtil.copyBytes(mno.message());
                    System.out.println("bytes = " + new String(bytes, UTF8));
                    offset = mno.offset();
                }
            }
        }

        // *** start the storm cluster
        LocalCluster cluster = new LocalCluster();

        // *** configure replay and karma to use the local kafka instance
        ReplayConfig replay = new ReplayConfig().staticHosts("localhost:9092", 1);
//        ReducerState state = prepReducerState("a");
        ReducerState state = new CassandraReducerState("catfish");
        KarmaConfig karmaConfig = new KarmaConfigImpl("a", replay, state);
//        KarmaConfig karmaConfig = new LocalKarmaConfig("a", cluster, broker, new HBaseReducerState());
//        KarmaConfig karmaConfig = new LocalKarmaConfig("a", cluster, broker, new HBaseReducerState());

        KarmaTopologyBuilder karma = new KarmaTopologyBuilder(karmaConfig, "catfish");

        karma.setSpout("tweets", new KafkaSpout(new ReplaySpoutConfig(replay, "tweets", new TweetKvScheme(
                L("id", "text", "userName", "location", "lang"),
                L("id_str", "text", "user.name", "user.location", "user.lang")
        ), "tweetsSpoutId")), 1).setMaxSpoutPending(100);

//        karma.map("{ text }", "tweets(id_str)", "{ s -> s.text.split('\\\\s+').each { emit(it.toLowerCase(), 1) } }")
//             .red("{ count }", "wordCount(word)", new Sum()).build();
//        buildSniffer(builder, "wordCount");

        karma.map("{ text }", "tweets(id)", new Count())
             .red("{ count }", "tweetCount()", new Sum());
        buildSniffer(karma, "tweetCount");

        karma.map("{ location }", "tweets(id)", new Count("location"))
             .red("{ count }", "locationCount(location)", new Sum());
//        buildSniffer(builder, "locationCount");

        karma.map("{ count location }", "locationCount(location)", new TopNMapper())
                .red("{ top5 }", "topLocation()", new TopN(5));
        buildSniffer(karma, "topLocation");

        karma.map("{ lang }", "tweets(id)", new Count("lang"))
             .red("{ count }", "langCount(lang)", new Sum());
//        buildSniffer(builder, "langCount", "default");

        karma.map("{ count lang }", "langCount(lang)", new TopNMapper())
             .red("{ top5 }", "topLang()", new TopN(5));
        buildSniffer(karma, "topLang");

        cluster.submitTopology("mytopo", new Config(), karma.createTopology());

        Utils.sleep(100000);
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
        builder.setBolt(srcComponentId + "Sniffer", new PrintBolt("### " + srcComponentId, 10000, false)).globalGrouping(srcComponentId, stream);
    }


}
