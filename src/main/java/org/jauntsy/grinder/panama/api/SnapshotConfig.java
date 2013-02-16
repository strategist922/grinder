package org.jauntsy.grinder.panama.api;

import backtype.storm.topology.BoltDeclarer;

import java.io.Serializable;
import java.util.Map;

/**
* User: ebishop
* Date: 12/26/12
* Time: 6:46 PM
*/
public interface SnapshotConfig extends Serializable {
    SnapshotAdapter buildStateAdapter(Map stormConfig);
    void configureGrouping(BoltDeclarer boltDeclarer, String componentId, String streamId);
}
