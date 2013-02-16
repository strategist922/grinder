package com.exacttarget.spike;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

import java.io.Serializable;
import java.util.*;

/**
 * User: ebishop
 * Date: 11/27/12
 * Time: 1:25 PM
 */
public class WordCountTopology implements Serializable {

    static Logger logger = Logger.getLogger(WordCountTopology.class);

    public static String KAFKA_BROKER_0_PUBLIC = "166.78.4.30";
    public static String KAFKA_BROKER_0_PRIVATE = "10.181.17.207";

    public static String ZK_0_PUBLIC = "198.61.203.155";
    public static String ZK_0_PRIVATE = "10.180.22.233";

    private static int MAX_ENTRIES_BEFORE_FLUSH = 10000;
    private static long MAX_TIME_BEFORE_FLUSH = 1000;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config config = new Config();
        config.setNumWorkers(32);
        config.setMaxSpoutPending(1000);
        config.setNumAckers(16);

        List<IRichSpout> spouts = new ArrayList();
//        IRichSpout spout = buildKafkaSpout("rackspace");
        for (int i = 0; i < 8; i++)
            spouts.add(new SimpleKafkaSpout(KAFKA_BROKER_0_PRIVATE, 9092, 100000, 64 * 1024, "test", i));
        StormTopology topology = new WordCountTopology().buildTopology("rackspace", spouts);
        StormSubmitter.submitTopology("WordCountTopology", config, topology);
    }

    public static class Local {
        public static void main(String[] args) {
            LocalCluster cluster = new LocalCluster();

            IRichSpout spout = new SimpleKafkaSpout(KAFKA_BROKER_0_PUBLIC, 9092, 100000, 64 * 1024, "test", 0);
            StormTopology topology = new WordCountTopology().buildTopology("test", Arrays.asList(spout));

            Config config = new Config();
            config.setMaxSpoutPending(10000);
            config.setDebug(false);
            cluster.submitTopology("wordCountTopology", config, topology);

            Utils.sleep(100000);

            cluster.shutdown();
        }
    }

    private StormTopology buildTopology(String env, List<IRichSpout> spouts) {
        TopologyBuilder builder = new TopologyBuilder();

        // *** setup kafka spouts
        for (int i = 0; i < spouts.size(); i++)
            builder.setSpout("kafkaSpout" + i, spouts.get(i));

        // *** add a tick spout, this is used by upstream spouts to trigger a flush
        // *** during low traffic situations.
        builder.setSpout("tick", new TickSpout(1000));

        // *** setup 8 splitter bolts
        BoltDeclarer splitter = builder.setBolt("splitter", new SplitterBolt(), 8);
        for (int i = 0; i < spouts.size(); i++)
            splitter.noneGrouping("kafkaSpout" + i);

        // *** setup 8 word count bolts
        BoltDeclarer counter = builder.setBolt("counter", new CounterBolt(env), 8);
        counter.fieldsGrouping("splitter", new Fields("word"));
        counter.allGrouping("tick");

        return builder.createTopology();
    }

    private static IRichSpout buildKafkaSpout(String env) {
        WordCountOptions options = environments.get(env);

        logger.info("WordCountTopo.buildKafkaSpout");
        logger.info("options.hostStrings = " + options.hostStrings.get(0));
        logger.info("options.partitionsPerBrokerHosts = " + options.partitionsPerHost);
        logger.info("options.zkServers.get(0) = " + options.zkServers.get(0));
        logger.info("options.zkPort = " + options.zkPort);

        SpoutConfig ksc = new SpoutConfig(
                SpoutConfig.StaticHosts.fromHostString(options.hostStrings, options.partitionsPerHost),
                "test", // <= topic
                "/storm-kafka",
                options.subscriptionId);
        ksc.zkServers = options.zkServers;
        ksc.zkPort = options.zkPort;
        ksc.scheme = new StringScheme();
        ksc.fetchSizeBytes = 256 * 1024;

        return new KafkaSpout(ksc);
    }

    static private final Map<String,WordCountOptions> environments = new HashMap() {{
        put("test", new WordCountOptions()
                .subscriptionId("wcSpout_test")
                .hostStrings(KAFKA_BROKER_0_PUBLIC + ":9092")
                .partitionsPerHost(8)
                .zkServers("localhost")
                .zkPort(2000)
        );
        put("rackspace", new WordCountOptions()
                .subscriptionId("wcSpout_prod")
                .hostStrings(KAFKA_BROKER_0_PRIVATE + ":9092")
                .partitionsPerHost(8)
                .zkServers(ZK_0_PRIVATE)
                .zkPort(2181)
        );
    }};

    private static Producer newRackspaceProducerForEnv(String env) {
        return newRackspaceProducerForBrokerList("0:" + environments.get(env).hostStrings.get(0));
    }

    private static Producer newRackspaceProducerForBrokerList(String brokerList) {
        logger.info("WordCountTopo.newRackspaceProducerForBrokerList");
        logger.info("brokerList = " + brokerList);
        Properties p = new Properties();
        p.setProperty("broker.list", brokerList);
        Producer<Integer,Message> producer = new Producer<Integer,Message>(new ProducerConfig(p));
        return producer;
    }

    static public class WordCountOptions implements Serializable {

        public String subscriptionId;
        public List<String> hostStrings;
        public int partitionsPerHost = 1;
        public List<String> zkServers;
        public int zkPort;

        public WordCountOptions hostStrings(String... hostStrings) {
            this.hostStrings = Arrays.asList(hostStrings);
            return this;
        }

        public WordCountOptions partitionsPerHost(int partitionsPerHost) {
            this.partitionsPerHost = partitionsPerHost;
            return this;
        }

        public WordCountOptions zkServers(String... zkServers) {
            this.zkServers = Arrays.asList(zkServers);
            return this;
        }

        public WordCountOptions zkPort(int zkPort) {
            this.zkPort = zkPort;
            return this;
        }

        public WordCountOptions subscriptionId(String id) {
            this.subscriptionId = id;
            return this;
        }
    }

    static public class SplitterBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String str = tuple.getString(0);
//            for (String part : str.split("\\s"))
            for (String part : fastSplit1(str))
                collector.emit(tuple, new Values(part));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

    static public class CounterBolt extends BaseRichBolt {

        private String env;

        private OutputCollector collector;
        private Producer producer;

        private Map<String,Integer> counts;
        private List<Tuple> pending;

        private long timeOfLastFlush;

        public CounterBolt(String env) {
            this.env = env;
        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
            this.collector = collector;
            this.producer = newRackspaceProducerForEnv(env);
            this.counts = new HashMap();
            this.pending = new ArrayList();
            this.timeOfLastFlush = System.currentTimeMillis();
        }

        @Override
        public void execute(Tuple tuple) {
            String source = tuple.getSourceComponent();
            if ("splitter".equals(source)) {
                String word = tuple.getString(0);
                Integer count = counts.get(word);
                count = count == null ? 1 : count + 1;
                counts.put(word, count);
                pending.add(tuple);
                if (pending.size() >= MAX_ENTRIES_BEFORE_FLUSH || System.currentTimeMillis() > timeOfLastFlush + MAX_TIME_BEFORE_FLUSH) {
                    flushPending();
                }
            } else {
                // *** a clock tick, flush if needed
                if (pending.size() > 0 && System.currentTimeMillis() > timeOfLastFlush + MAX_TIME_BEFORE_FLUSH) {
                    flushPending();
                }
                collector.ack(tuple);
            }
        }

        private void flushPending() {
            if (pending.size() < 1)
                return;

            Set<String> words = new TreeSet<String>();
            for (Tuple t : pending) {
                words.add(t.getString(0));
            }
            List<ProducerData> list = new ArrayList<ProducerData>();
            for (String word : words) {
                String str = word + " " + counts.get(word);
                Message message = new Message(str.getBytes());
                ProducerData data = new ProducerData("wc", 0, Arrays.asList(message));
                list.add(data);
            }
            producer.send(list);
            for (Tuple t : pending) {
                collector.ack(t);
            }
            pending.clear();

            timeOfLastFlush = System.currentTimeMillis();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

    }

    public static String[] fastSplit(String s) {
        List<String> values = new ArrayList<String>();
        int wordStart = -1;
        int len = s.length();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                if (wordStart != -1) {
                    values.add(s.substring(wordStart, i));
                    wordStart = -1;
                }
            } else {
                if (wordStart == -1) {
                    wordStart = i;
                }
            }
        }
        if (wordStart != -1)
            values.add(s.substring(wordStart, len));
        String[] result = new String[values.size()];
        return values.toArray(result);
    }

    public static String[] fastSplit1(String s) {
        String[] values = new String[s.length() / 2];
        int v = 0;
        int wordStart = -1;
        int len = s.length();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                if (wordStart != -1) {
                    values[v++] = s.substring(wordStart, i);
                    wordStart = -1;
                }
            } else {
                if (wordStart == -1) {
                    wordStart = i;
                }
            }
        }
        if (wordStart != -1)
            values[v++] = s.substring(wordStart, len);
//        String[] result = new String[values.size()];
//        return values.toArray(result);
        return Arrays.copyOf(values, v, String[].class);
    }

}
