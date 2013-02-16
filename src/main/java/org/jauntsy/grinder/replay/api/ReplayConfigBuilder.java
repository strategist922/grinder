package org.jauntsy.grinder.replay.api;

/**
* User: ebishop
* Date: 1/4/13
* Time: 10:36 AM
*/
public class ReplayConfigBuilder extends ReplayConfig<ReplayConfigBuilder> {

    public ReplayConfig buildReplayConfig() {
        return new ReplayConfig(brokerHosts, zkRoot, archiveConfig);
    }

}
