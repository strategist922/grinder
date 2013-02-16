package org.jauntsy.grinder.replay.api;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaScheme;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.SpoutConfig;

/**
 * User: ebishop
 * Date: 12/19/12
 * Time: 5:04 PM
 */
public class ReplaySpoutConfig<T extends ReplaySpoutConfig> extends SpoutConfig {

    public ReplaySpoutConfig(ReplayConfig replay, String topic, Scheme scheme, String consumerId) {
        this(replay.getBrokerHosts(), topic, replay.getZkRoot(topic), replay.archiveConfig, consumerId);
        scheme(scheme);
        forceFromStart();
    }

    public ReplaySpoutConfig(ReplayTopicConfig replayTopic, String consumerId) {
        this(replayTopic.getBrokerHosts(), replayTopic.getTopic(), replayTopic.getZkRoot(), replayTopic.getArchiveConfig(), consumerId);
    }

    public ReplaySpoutConfig(BrokerHosts brokerHosts, String topic, String zkRoot, ArchiveConfig archive, String consumerId) {
        super(brokerHosts, topic, zkRoot, consumerId);    //To change body of overridden methods use File | Settings | File Templates.
        this.kafkaScheme = new KafkaScheme(new RawScheme(), false);
        this.archive = archive;
    }

    public T scheme(Scheme scheme) {
        return scheme(new KafkaScheme(scheme, false));
    }

    public T scheme(KafkaScheme scheme) {
        this.kafkaScheme = scheme;
        return (T)this;
    }

    public T forceFromStart() {
        this.forceFromStart = true;
        return (T)this;
    }

}
