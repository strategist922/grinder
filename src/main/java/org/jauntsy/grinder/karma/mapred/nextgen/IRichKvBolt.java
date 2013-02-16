package org.jauntsy.grinder.karma.mapred.nextgen;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * User: ebishop
 * Date: 2/11/13
 * Time: 10:47 AM
 */
public interface IRichKvBolt {

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    public void execute(Tuple input);

    public void cleanup();

    public void declareOutputFields(KvOutputFieldsDeclarer declarer);

    public Map<String, Object> getComponentConfiguration();

}
