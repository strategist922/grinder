package org.jauntsy.grinder.panama.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.panama.api.ClusterConfig;
import org.jauntsy.grinder.panama.api.Dbo;
import org.jauntsy.grinder.panama.api.ClusterWriter;

import java.util.List;
import java.util.Map;

/**
* User: ebishop
* Date: 12/26/12
* Time: 2:02 PM
*/
public class SubscriberBolt extends BaseRichBolt {

    private ClusterConfig cluster;

    private transient ClusterWriter incoming;
    private transient OutputCollector collector;

    public SubscriberBolt(ClusterConfig cluster) {
        this.cluster = cluster;
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.incoming = cluster.buildWriter();
        this.collector = collector;
    }

    @Override
    public void cleanup() {
        this.incoming.cleanup();
    }

    @Override
    public void execute(Tuple tuple) {
        String table = tuple.getString(0);

        List key = (List)tuple.getValue(1);

        Dbo update = new Dbo(key);
        update.merge((List) tuple.getValue(2));

        long timestamp = tuple.getLong(3);

        String uuid = tuple.getString(4);

        incoming.put(table, update, timestamp, uuid);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
