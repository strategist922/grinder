package org.jauntsy.grinder.karma.operations.map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.karma.Emitter;
import org.jauntsy.grinder.karma.mapred.Mapper;

/**
 * User: ebishop
 * Date: 1/9/13
 * Time: 1:42 PM
 */
public class Count extends Mapper {

    private final Fields groupFields;

    public Count(String... groupFields) {
        this.groupFields = new Fields(groupFields);
    }

    @Override
    public void map(Tuple t, Emitter e) {
        Object[] values = new Object[groupFields.size() + 1];
        for (int i = 0; i < values.length - 1; i++)
            values[i] = t.getValueByField(groupFields.get(i));
        values[values.length - 1] = 1L;
        e.emit(values);
    }
}
