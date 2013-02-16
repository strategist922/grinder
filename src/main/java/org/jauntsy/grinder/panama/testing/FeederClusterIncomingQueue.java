package org.jauntsy.grinder.panama.testing;

import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.jauntsy.grinder.panama.api.*;

import java.util.List;

/**
* User: ebishop
* Date: 12/11/12
* Time: 7:24 PM
*/
public class FeederClusterIncomingQueue implements ClusterIncomingQueue {

    private FeederSpout spout;

    public FeederClusterIncomingQueue() {
        this.spout = new FeederSpout(new Fields("table", "key", "update", "timestamp", "uuid"));
    }

    @Override
    public ClusterWriter buildWriter() {
        return new BaseClusterWriter() {
            @Override
            public void putBatch(List<Update> updates) {
                for (Update update : updates) {
                    spout.feed(
                        new Values(
                                update.table,
                                update.value.getId(),
                                update.value.toList(false),
                                update.timestamp,
                                update.uuid
                        )
                    );
                }
            }
        };
    }

    @Override
    public IRichSpout buildSpout(String id) {
        return this.spout;
    }

}
