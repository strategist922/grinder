package org.jauntsy.grinder.karma;

import org.jauntsy.grinder.karma.mapred.*;
import org.jauntsy.grinder.replay.api.ReplayConfig;

/**
 * User: ebishop
 * Date: 1/2/13
 * Time: 12:18 PM
 */
public class KarmaConfig {

    private String ns;
//    private SpoutConfig.BrokerHosts brokerHosts;
//    private List brokerHosts;
//    private int partitionsPerHost;
    private String zkConnect;
    private ReducerState reducerState;
    private ReplayConfig replayConfig;

    public KarmaConfig(String ns) {
        this.ns = ns;
    }

    public KarmaConfig replay(ReplayConfig replayConfig) {
        this.replayConfig = replayConfig;
//        this.brokerHosts = replayConfig.getBrokerHosts();
        return this;
    }

//    public KarmaConfig staticHosts(List<String> brokerHostPortStrings, int partitionsPerHost) {
////        this.brokerHosts = Arrays.asList(brokerHostPortStrings);
////        this.partitionsPerHost = partitionsPerHost;
//        this.brokerHosts = KafkaConfig.StaticHosts.fromHostString(brokerHostPortStrings, partitionsPerHost);
//        return this;
//    }

    public KarmaConfig zkConnect(String zkConnect) {
        this.zkConnect = zkConnect;
        return this;
    }

    public KarmaConfig reducerState(ReducerState reducerState) {
        this.reducerState = reducerState;
        return this;
    }

    public ReducerState getReducerState() {
        return reducerState;
    }

    public String getNs() {
        return ns;
    }

    public ReplayConfig getReplayConfig() {
        return replayConfig;
    }
}
