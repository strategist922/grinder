package org.jauntsy.grinder.panama.base;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.tuple.Fields;
import org.jauntsy.grinder.panama.api.SnapshotConfig;

/**
 * User: ebishop
 * Date: 12/27/12
 * Time: 7:21 PM
 */
public abstract class BaseSnapshotConfig implements SnapshotConfig {

    @Override
    public void configureGrouping(BoltDeclarer boltDeclarer, String componentId, String streamId) {
        boltDeclarer.grouping(new GlobalStreamId(componentId, streamId), Grouping.fields(new Fields("key").toList()));
    }

}
