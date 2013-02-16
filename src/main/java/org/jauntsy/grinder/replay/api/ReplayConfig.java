package org.jauntsy.grinder.replay.api;

import backtype.storm.generated.StormTopology;
import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.jauntsy.grinder.replay.impl.ArchivistBolt;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaConfig;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaScheme;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaSpout;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.SpoutConfig;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * User: ebishop
 * Date: 12/19/12
 * Time: 11:37 AM
 */
public class ReplayConfig<T extends ReplayConfig> implements Serializable {

    protected KafkaConfig.BrokerHosts brokerHosts;
    protected String zkRoot;
    protected ArchiveConfig archiveConfig;

    private static String defaultZkRoot() {
        return "/replay/consumers";
    }

    public ReplayConfig() {
        this.zkRoot = defaultZkRoot();
    }

    public ReplayConfig(KafkaConfig.BrokerHosts brokerHosts, ArchiveConfig archiveConfig) {
        this(brokerHosts, (String)null, archiveConfig);
    }

    public ReplayConfig(KafkaConfig.BrokerHosts brokerHosts, String zkRoot, ArchiveConfig archiveConfig) {
        this.brokerHosts = brokerHosts;
        this.zkRoot = zkRoot == null ? defaultZkRoot() : zkRoot;
        this.archiveConfig = archiveConfig;
    }

    public StormTopology buildTopicArchiveTopology(String topic, int maxSpoutPending) {
        TopologyBuilder archiveBuilder = new TopologyBuilder();
        buildTopicArchiveTopology(archiveBuilder, topic, maxSpoutPending);
        return archiveBuilder.createTopology();
    }

    /*
    TODO: add parallelism to spouts and bolts, also route based on kafka hash
     */
    public void buildTopicArchiveTopology(TopologyBuilder archivistTopologyBuilder, String topic, int maxSpoutPending) {
        String spoutName = topic + "_archiveBuilderSpout";
        archivistTopologyBuilder.setSpout(spoutName, new KafkaSpout(buildArchivistSpoutConfig(topic, topic + "_archivist"))).setMaxSpoutPending(maxSpoutPending);

        String boltName = topic + "_archiveBuilderBolt";
        archivistTopologyBuilder.setBolt(boltName, new ArchivistBolt(this.archiveConfig), 8) // *** why 8? don't hardcode this
                .fieldsGrouping(spoutName, new Fields("topic", "hostname", "port", "partition"));
    }

    private SpoutConfig buildArchivistSpoutConfig(String topic, String id) {
        SpoutConfig config = new SpoutConfig(brokerHosts, topic, getZkRoot(topic), id);
        config.kafkaScheme = new KafkaScheme(new RawScheme(), true);
        config.forceFromStart = true;
        return config;
    }

    public SpoutConfig buildReplaySpoutConfig(String topic, String id) {
        return buildReplaySpoutConfig(topic, new KafkaScheme(new RawScheme(), true), id);
    }

    public SpoutConfig buildReplaySpoutConfig(String topic, Scheme scheme, String id) {
        return buildReplaySpoutConfig(topic, new KafkaScheme(scheme, false), id);
    }

    public SpoutConfig buildReplaySpoutConfig(String topic, KafkaScheme scheme, String id) {
        SpoutConfig config = new SpoutConfig(brokerHosts, topic, getZkRoot(topic), id);
        config.kafkaScheme = scheme;
        config.archive = archiveConfig;
        config.forceFromStart = true;
        return config;
    }

    public KafkaConfig.BrokerHosts getBrokerHosts() {
        return brokerHosts;
    }

    public String getZkRoot(String topic) {
        return zkRoot  + "/" + topic;
    }

    public ArchiveConfig getArchiveConfig() {
        return archiveConfig;
    }

    public T staticHosts(List<String> hostPortStrings, int partitionsPerHost) {
        this.brokerHosts = KafkaConfig.StaticHosts.fromHostString(hostPortStrings, partitionsPerHost);
        return (T)this;
    }

    public T staticHosts(String[] hostPortStrings, int partitionsPerHost) {
        this.brokerHosts = KafkaConfig.StaticHosts.fromHostString(Arrays.asList(hostPortStrings), partitionsPerHost);
        return (T)this;
    }

    public T staticHosts(String hostPortCsvStrings, int partitionsPerHost) {
        return staticHosts(hostPortCsvStrings.split(","), partitionsPerHost);
    }

    public T dynamicHosts(String brokerZkStr) {
        return dynamicHosts(brokerZkStr, "/brokers");
    }

    public T dynamicHosts(String brokerZkStr, String brokerZkPath) {
        this.brokerHosts = new KafkaConfig.ZkHosts(brokerZkStr, brokerZkPath);
        return (T)this;
    }

    public T archive(ArchiveConfig archiveConfig) {
        this.archiveConfig = archiveConfig;
        return (T)this;
    }

}
