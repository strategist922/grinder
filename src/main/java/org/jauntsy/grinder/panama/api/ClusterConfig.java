package org.jauntsy.grinder.panama.api;

import backtype.storm.generated.StormTopology;
import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.exacttarget.kafka.config.SyncProducerBuilder;
import org.jauntsy.grinder.panama.bolts.*;
import org.jauntsy.grinder.panama.kafka.KafkaChangesQueue;
import org.jauntsy.grinder.panama.kafka.KafkaClusterIncomingQueue;
import org.jauntsy.grinder.panama.kafka.scheme.ArchiveScheme;
import org.jauntsy.grinder.panama.kafka.scheme.UpdateScheme;
import org.jauntsy.grinder.replay.api.ArchiveConfig;
import org.jauntsy.grinder.replay.api.ReplayTopicConfig;
import org.jauntsy.grinder.replay.api.ReplaySpoutConfig;
import org.jauntsy.grinder.replay.impl.ArchivistBolt;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaConfig;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaScheme;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaSpout;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.SpoutConfig;
import kafka.message.Message;
import scala.actors.threadpool.Arrays;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * User: ebishop
 * Date: 12/6/12
 * Time: 5:40 PM
 */
public class ClusterConfig implements Serializable {

    private static final int DEFAULT_MAX_SPOUT_PENDING = 1000;

    private String ns;

    private KafkaConfig.BrokerHosts brokerHosts;

    private ArchiveConfig archiveConfig;

    private SnapshotConfig snapshotConfig;

    private Integer numCurators = 1;
    private String zkConnect;

    public ClusterConfig(String ns) {
        this.ns = ns;
    }

    public ClusterConfig numCurators(int numCurators) {
        this.numCurators = numCurators;
        return this;
    }

    private String buildClusterPath(String relativePath) {
        return new StringBuilder("/dbq/cluster/").append(ns).append(relativePath).toString();
    }

    private ClusterIncomingQueue getOrCreateIncomingQueue() {
        return new KafkaClusterIncomingQueue(
                ns,
                getIncomingProducerProperties(),
                getIncomingSpoutConfig()
        );
    }

    public ClusterConfig snapshot(SnapshotConfig snapshotConfig) {
        this.snapshotConfig = snapshotConfig;
        return this;
    }

    public ClusterConfig archive(ArchiveConfig archiveConfig) {
        this.archiveConfig = archiveConfig;
        return this;
    }

    public StormTopology buildClusterTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        buildCurator(builder);
        buildChangesArchivist(builder, "archivist");
        return builder.createTopology();
    }

    public StormTopology buildQueueArchivistTopology(String topic) {
        return buildQueueArchivistTopology(topic, DEFAULT_MAX_SPOUT_PENDING);
    }

    public SpoutConfig buildReplaySpoutConfig(String topic, Scheme scheme, String consumerId) {
        return new ReplaySpoutConfig(getReplayConfig(topic), consumerId)
                .scheme(scheme)
                .forceFromStart();
    }

    public KafkaSpout buildReplaySpout(String topic, Scheme scheme, String consumerId) {
        return new KafkaSpout(buildReplaySpoutConfig(topic, scheme, consumerId));
    }

    private KafkaConfig.BrokerHosts getBrokerHosts() {
        return brokerHosts;
    }

    private ReplayTopicConfig getReplayConfig(String topic) {
        return new ReplayTopicConfig(getBrokerHosts(), topic, archiveConfig);
    }

    public void buildQueueArchivistTopology(TopologyBuilder builder, String topic, int maxSpoutPending) {
        getReplayConfig(topic).buildTopicArchiveTopology(builder, maxSpoutPending);
    }

    public StormTopology buildQueueArchivistTopology(String topic, int maxSpoutPending) {
        TopologyBuilder builder = new TopologyBuilder();
        buildQueueArchivistTopology(builder, topic, maxSpoutPending);
        return builder.createTopology();
    }

    public void buildCurator(TopologyBuilder builder) {
        validate();

        String incomingSpout = ns + "_incoming";
        builder.setSpout(incomingSpout, buildIncomingSpout());

//        String tickSpout = ns + "_tick";
//        builder.setSpout(tickSpout, new TickSpout(200));

        String curatorBolt = ns + "_curator";
        BoltDeclarer curatorBoltDeclarer = builder.setBolt(
                curatorBolt,
                new CuratorBolt4(snapshotConfig, buildChangesWriter()),
                numCurators
        );
        snapshotConfig.configureGrouping(curatorBoltDeclarer, incomingSpout, Utils.DEFAULT_STREAM_ID);
//        curatorBoltDeclarer.allGrouping(tickSpout, "tick");
    }

    private ChangesWriter buildChangesWriter() {
        return getOrCreateChangesQueue().buildWriter();
    }

    private transient KafkaChangesQueue _changesQueue = null;

    private KafkaChangesQueue getOrCreateChangesQueue() {
        if (_changesQueue == null) {
            _changesQueue = new KafkaChangesQueue(
                    ns,
                    getReplayConfig(ns + "_changes"),
                    getChangesProducerProperties()
            );
        }
        return _changesQueue;
    }

    private IRichSpout buildIncomingSpout() {
        return new KafkaSpout(getIncomingSpoutConfig());
    }

    private void buildChangesArchivist(TopologyBuilder builder, String id) {
        buildClusterTopicArchivist(builder, ns + "_changes", buildClusterPath("/_changes/consumers"), DEFAULT_MAX_SPOUT_PENDING, id);
    }

    private void buildClusterTopicArchivist(TopologyBuilder builder, String topic, String zkRoot, int maxSpoutPending, String id) {
        validate();
        if (archiveConfig != null) {
            String spoutName = topic + "_archiveBuilderSpout";
            SpoutConfig config = new SpoutConfig(getBrokerHosts(), topic, zkRoot, id);
            config.kafkaScheme = new KafkaScheme(new RawScheme(), true);
            config.forceFromStart = true;
            builder.setSpout(spoutName, new KafkaSpout(config)).setMaxSpoutPending(maxSpoutPending);

            String boltName = topic + "_archiveBuilderBolt";
            builder.setBolt(boltName, new ArchivistBolt(archiveConfig), 8)
                    .fieldsGrouping(spoutName, new Fields("topic", "hostname", "port", "partition"));
        }
    }


    private void validate() {
        if (ns == null) throw new IllegalArgumentException("id required");
//        if (incomingProducerProperties == null) throw new IllegalArgumentException("incomingProducer required");
        if (snapshotConfig == null) throw new IllegalArgumentException("state required");
//        if (changesQueueProducerProperties == null) throw new IllegalArgumentException("changesQueueProducerProperties required");
        if (brokerHosts == null) throw new IllegalArgumentException("brokerHosts required");
//        if (partitionsPerHost == null) throw new IllegalArgumentException("partitionsPerBrokerHosts required");
//        if (zkConnect == null) throw new IllegalArgumentException("zkConnect required");
    }


    public ClusterWriter buildWriter() {
        return getOrCreateIncomingQueue().buildWriter();
    }

    public SnapshotAdapter buildStateClient(Map config) {
        return snapshotConfig.buildStateAdapter(config);
    }

    public StormTopology buildSubscriptionFrom(ClusterConfig sourceCluster) {
        TopologyBuilder builder = new TopologyBuilder();

        String scope = ns + "_sub_" + sourceCluster.getNs();
        String subsSpout = scope + "_reader";
        builder.setSpout(
                subsSpout,
                sourceCluster.buildSubscriptionSpout(buildClusterPath("/replication/from"), sourceCluster.getNs()),
                1
        ).setMaxSpoutPending(DEFAULT_MAX_SPOUT_PENDING);

        String subsBolt = scope + "_writer";
        builder.setBolt(subsBolt, new SubscriberBolt(this), 1).fieldsGrouping(subsSpout, new Fields("key"));

        return builder.createTopology();
    }

    private IRichSpout buildSubscriptionSpout(String zkRoot, String id) {
        ReplayTopicConfig replayTopicConfig = new ReplayTopicConfig(getBrokerHosts(), ns + "_changes", zkRoot, archiveConfig);
        ReplaySpoutConfig spoutConfig = new ReplaySpoutConfig(replayTopicConfig, id)
                .scheme(new ArchiveScheme());
        return new KafkaSpout(spoutConfig);
        //return getOrCreateChangesQueue().buildSpout(id);
    }

    public String getNs() {
        return ns;
    }

    public ClusterConfig staticHosts(String[] brokerHosts, int partitionsPerHost) {
        this.brokerHosts = KafkaConfig.StaticHosts.fromHostString(Arrays.asList(brokerHosts), partitionsPerHost);
        return this;
    }

    public ClusterConfig dynamicHosts(String brokerZkStr) {
        this.brokerHosts = new KafkaConfig.ZkHosts(brokerZkStr, "/brokers");
        return this;
    }

    public ClusterConfig dynamicHosts(String brokerZkStr, String brokerZkPath) {
        this.brokerHosts = new KafkaConfig.ZkHosts(brokerZkStr, brokerZkPath);
        return this;
    }

//    public ClusterConfig partitionsPerBrokerHosts(int numPartitions) {
//        this.partitionsPerHost = numPartitions;
//        return this;
//    }
//
    public ClusterConfig zkConnect(String zkConnect) {
        this.zkConnect = zkConnect;
        return this;
    }

    private SpoutConfig getIncomingSpoutConfig() {
        SpoutConfig incomingSpoutConfig = new SpoutConfig(
                getBrokerHosts(),
                ns + "_incoming",
                buildClusterPath("/_incoming/consumers"),
                "curator"
        );
        incomingSpoutConfig.kafkaScheme = new KafkaScheme(new UpdateScheme());
        incomingSpoutConfig.forceFromStart = true;
        return incomingSpoutConfig;
    }

    public Properties getChangesProducerProperties() {
        return new SyncProducerBuilder<Long, Message>().zkConnect(zkConnect);
    }

    public Properties getIncomingProducerProperties() {
        return new SyncProducerBuilder<Long, Message>().zkConnect(zkConnect);
    }

    public StateReader buildReader(Map config) {
        final SnapshotAdapter adapter = snapshotConfig.buildStateAdapter(config);
        return new StateReader() {
            @Override
            public Dbo get(String table, Dbo query) {
                Dbv one = adapter.getOne(table, query.getId());
                return one == null ? null : one.toDoc();
            }

            @Override
            public void close() {
                adapter.close();
            }
        };
    }
}
