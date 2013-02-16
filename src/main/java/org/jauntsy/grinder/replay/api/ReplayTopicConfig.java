package org.jauntsy.grinder.replay.api;

import backtype.storm.generated.StormTopology;
import backtype.storm.spout.RawScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.jauntsy.grinder.replay.impl.ArchivistBolt;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaConfig;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaScheme;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaSpout;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.SpoutConfig;

import java.io.Serializable;
import java.util.Arrays;

/**
 * User: ebishop
 * Date: 12/19/12
 * Time: 11:37 AM
 */
public class ReplayTopicConfig<T extends ReplayTopicConfig> implements Serializable {

    protected KafkaConfig.BrokerHosts brokerHosts;
    protected String topic;
    protected String zkRoot;
    protected ArchiveConfig archiveConfig;

    private static String defaultZkRoot(String topic) {
        return "/replay/topics/" + topic + "/consumers";
    }

    public ReplayTopicConfig() {

    }

    public ReplayTopicConfig(KafkaConfig.BrokerHosts brokerHosts, String topic, ArchiveConfig archiveConfig) {
        this(brokerHosts, topic, (String)null, archiveConfig);
    }

    public ReplayTopicConfig(KafkaConfig.BrokerHosts brokerHosts, String topic, String zkRoot, ArchiveConfig archiveConfig) {
        this.brokerHosts = brokerHosts;
        this.topic = topic;
        this.zkRoot = zkRoot == null ? defaultZkRoot(topic) : zkRoot;
        this.archiveConfig = archiveConfig;
    }

    public StormTopology buildTopicArchiveTopology(int maxSpoutPending) {
        TopologyBuilder archiveBuilder = new TopologyBuilder();
        buildTopicArchiveTopology(archiveBuilder, maxSpoutPending);
        return archiveBuilder.createTopology();
    }

    /*
    TODO: add parallelism to spouts and bolts, also route based on kafka hash
     */
    public void buildTopicArchiveTopology(TopologyBuilder archivistTopologyBuilder, int maxSpoutPending) {
        String spoutName = topic + "_archiveBuilderSpout";
        archivistTopologyBuilder.setSpout(spoutName, new KafkaSpout(buildArchivistSpoutConfig(topic + "_archivist"))).setMaxSpoutPending(maxSpoutPending);

        String boltName = topic + "_archiveBuilderBolt";
        archivistTopologyBuilder.setBolt(boltName, new ArchivistBolt(this.archiveConfig), 8) // *** why 8? don't hardcode this
                .fieldsGrouping(spoutName, new Fields("topic", "hostname", "port", "partition"));
    }

    private SpoutConfig buildArchivistSpoutConfig(String id) {
        SpoutConfig config = new SpoutConfig(brokerHosts, topic, getZkRoot(), id);
        config.kafkaScheme = new KafkaScheme(new RawScheme(), true);
        config.forceFromStart = true;
        return config;
    }

    public SpoutConfig buildReplaySpoutConfig(String id) {
        SpoutConfig config = new SpoutConfig(brokerHosts, topic, getZkRoot(), id);
        config.kafkaScheme = new KafkaScheme(new RawScheme(), true);
        config.archive = archiveConfig;
        config.forceFromStart = true;
        return config;
    }

    public KafkaConfig.BrokerHosts getBrokerHosts() {
        return brokerHosts;
    }

    public String getTopic() {
        return topic;
    }

    public String getZkRoot() {
        return zkRoot != null ? zkRoot : defaultZkRoot(topic);
    }

    public ArchiveConfig getArchiveConfig() {
        return archiveConfig;
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

    public T topic(String topic) {
        this.topic = topic;
        return (T)this;
    }

    public T archive(ArchiveConfig archiveConfig) {
        this.archiveConfig = archiveConfig;
        return (T)this;
    }

    public static class Builder extends ReplayTopicConfig<Builder> {

        public ReplayTopicConfig buildReplayTopicConfig() {
            return new ReplayTopicConfig(brokerHosts, topic, zkRoot, archiveConfig);
        }

    }

}
