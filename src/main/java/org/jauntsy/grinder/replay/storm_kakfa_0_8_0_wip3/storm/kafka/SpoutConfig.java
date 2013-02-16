package org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka;

import org.jauntsy.grinder.replay.api.ArchiveConfig;

import java.io.Serializable;
import java.util.List;


public class SpoutConfig extends KafkaConfig implements Serializable {
    public List<String> zkServers = null;
    public Integer zkPort = null;
    public String zkRoot = null;
    public String id = null;
    public long stateUpdateIntervalMs = 2000;
    public ArchiveConfig archive;

    public SpoutConfig(BrokerHosts hosts, String topic, String zkRoot, String id) {
        super(hosts, topic);
        this.zkRoot = zkRoot;
        this.id = id;
    }
}
