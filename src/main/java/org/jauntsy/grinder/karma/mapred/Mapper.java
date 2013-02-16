package org.jauntsy.grinder.karma.mapred;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.karma.Emitter;

import java.io.Serializable;
import java.util.Map;

/**
 * User: ebishop
 * Date: 1/3/13
 * Time: 2:26 PM
 */
public abstract class Mapper implements Serializable {

    public abstract void map(Tuple doc, Emitter e);

    public void prepare(Map stormConfig, TopologyContext context) {

    }

}
