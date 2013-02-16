package org.jauntsy.grinder.panama;

import backtype.storm.generated.StormTopology;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.SpoutConfig;

/**
 * User: ebishop
 * Date: 12/6/12
 * Time: 5:32 PM
 */
public class Subscription {

    private final SpoutConfig destKafkaConfig;
    private final String destNs;
    private final String destTopic;

    private final SpoutConfig srcKafkaConfig;
    private final String srcNs;
    private final String srcTopic;

    public Subscription(
            SpoutConfig destKafkaConfig,
            String destNs,
            String destTopic,
            SpoutConfig srcKafkaConfig,
            String srcNs,
            String srcTopic) {
        this.destKafkaConfig = destKafkaConfig;
        this.destNs = destNs;
        this.destTopic = destTopic;

        this.srcKafkaConfig = srcKafkaConfig;
        this.srcNs = srcNs;
        this.srcTopic = srcTopic;
    }

    public StormTopology buildTopology() {
        return null;
    }

}
