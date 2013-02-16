package org.jauntsy.grinder.karma.demo;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ebishop
 * Date: 2/6/13
 * Time: 9:28 AM
 */
public abstract class KvScheme implements Scheme {

    private final List<String> keyFields;
    private final List<String> valueFields;
    private final ArrayList<String> outputFields;

    public KvScheme(List<String> keyFields, List<String> valueFields) {
        this.keyFields = keyFields;
        this.valueFields = valueFields;
        this.outputFields = new ArrayList<String>();
        outputFields.addAll(keyFields);
        outputFields.addAll(valueFields);
    }

    public final List<Object> deserialize(byte[] ser) {
        List ret = buildRow(ser);
        if (ret == null)
            return null;
        for (int i = 0; i < keyFields.size(); i++) {
            if (ret.get(i) == null) {
                return null;
            }
        }
        return ret;
    }

    public abstract List buildRow(byte[] ser);

    @Override
    public Fields getOutputFields() {
        return new Fields(outputFields);
    }

}
