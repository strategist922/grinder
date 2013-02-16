package org.jauntsy.grinder.karma.mapred;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 1/9/13
 * Time: 11:39 AM
 */
public abstract class Formatter implements Serializable {

    public abstract List merge(Tuple t);

    public void prepare(Map stormConfig, TopologyContext context) {

    }

}
