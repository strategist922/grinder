package org.jauntsy.grinder.panama.testing;

import org.jauntsy.grinder.panama.api.SnapshotAdapter;
import org.jauntsy.grinder.panama.base.BaseSnapshotConfig;

import java.util.Map;

/**
* User: ebishop
* Date: 12/26/12
* Time: 6:47 PM
*/
public class MemorySnapshotConfig extends BaseSnapshotConfig {

    private String ns;

    public MemorySnapshotConfig(String ns) {
        this.ns = ns;
    }

    @Override
    public SnapshotAdapter buildStateAdapter(Map stormConfig) {
        return new MemorySnapshotAdapter(ns);
    }

}
