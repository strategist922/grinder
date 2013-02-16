package org.jauntsy.grinder.karma;

import org.jauntsy.grinder.karma.mapred.ReducerState;
import org.jauntsy.grinder.replay.api.ReplayConfig;

/**
 * User: ebishop
 * Date: 1/16/13
 * Time: 9:04 PM
 */
public class KarmaConfigImpl extends KarmaConfig {

    public KarmaConfigImpl(String ns, ReplayConfig replayConfig, ReducerState reducerState) {
        super(ns);
        this.replay(replayConfig);
        this.reducerState(reducerState);
    }

}
