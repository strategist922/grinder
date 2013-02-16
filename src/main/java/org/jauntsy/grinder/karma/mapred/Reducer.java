package org.jauntsy.grinder.karma.mapred;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 1/3/13
 * Time: 2:26 PM
 */
public abstract class Reducer implements Serializable {

    public abstract List reduce(Tuple key, Tuple a, Tuple b);

    public void prepare(Map stormConfig, TopologyContext context) {
    }

}
