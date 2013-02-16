package org.jauntsy.grinder.panama;

import backtype.storm.generated.StormTopology;
import org.jauntsy.grinder.panama.api.SnapshotAdapter;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.SpoutConfig;

/**
 * User: ebishop
 * Date: 12/6/12
 * Time: 2:04 PM
 */
public class Datacenter {

    private final String ns;
    private SpoutConfig kafkaConfig;
    private final SnapshotAdapter snapshotAdapter;

    public Datacenter(String ns, SpoutConfig kafkaConfig, SnapshotAdapter snapshotAdapter) {
        this.ns = ns;
        this.kafkaConfig = kafkaConfig;
        this.snapshotAdapter = snapshotAdapter;
    }

    public StormTopology deploy() {
        // *** build _incoming
        // *** build _archivist
        return null;
    }


}
