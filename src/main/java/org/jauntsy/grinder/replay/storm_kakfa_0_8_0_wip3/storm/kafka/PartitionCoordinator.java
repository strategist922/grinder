package org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka;

import java.util.List;

public interface PartitionCoordinator {
    List<PartitionManager> getMyManagedPartitions();
    PartitionManager getManager(GlobalPartitionId id);
}
