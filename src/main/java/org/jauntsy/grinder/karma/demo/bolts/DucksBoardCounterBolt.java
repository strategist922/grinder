package org.jauntsy.grinder.karma.demo.bolts;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.IOException;

import static org.jauntsy.nice.Nice.M;

/**
 * User: ebishop
 * Date: 2/8/13
 * Time: 2:16 PM
 */
public class DucksBoardCounterBolt extends DuckBoardBolt {

    private final String counterField;

    public DucksBoardCounterBolt(Fields idFields, String widgetId, String counterField) {
        super(idFields, widgetId);
        this.counterField = counterField;
    }

    @Override
    protected void updateOne(Tuple tuple) throws IOException {
        post(M("value", getValue(tuple)));
    }

    private Number getValue(Tuple input) {
        Number value;
        try {
            value = (Number)input.getValueByField(counterField);
            if (value == null)
                value = 0;
        } catch(Exception ex) {
            value = 0;
        }
        return value;
    }


}
