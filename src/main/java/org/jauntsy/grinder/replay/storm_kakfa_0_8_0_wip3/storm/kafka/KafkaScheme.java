package org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * User: ebishop
 * Date: 12/18/12
 * Time: 4:11 PM
 */
public class KafkaScheme implements Serializable {

    private Scheme delegate;
    private boolean includeMeta;

    public KafkaScheme(Scheme delegate) {
        this(delegate, false);
    }

    public KafkaScheme(Scheme delegate, boolean includeMeta) {
        this.delegate = delegate;
        this.includeMeta = includeMeta;
    }

    public List<Object> deserialize(String topic, String hostname, int port, int partition, long requestOffset,  long nextOffset, byte[] bytes) {
        List<Object> value = delegate.deserialize(bytes);
        if (value == null)
            return null;
        List<Object> ret = new ArrayList<Object>();
        if (includeMeta) {
            ret.add(topic);
            ret.add(hostname);
            ret.add(port);
            ret.add(partition);
            ret.add(requestOffset);
            ret.add(nextOffset);
        }
        ret.addAll(value);
        return ret;
    }

    public Fields getOutputFields() {
        List<String> fields = new ArrayList<String>();
        if (includeMeta) {
            fields.add("topic");
            fields.add("hostname");
            fields.add("port");
            fields.add("partition");
            fields.add("offset");
            fields.add("nextOffset");
        }
        for (String field : delegate.getOutputFields())
            fields.add(field);
        return new Fields(fields);
    }

}
