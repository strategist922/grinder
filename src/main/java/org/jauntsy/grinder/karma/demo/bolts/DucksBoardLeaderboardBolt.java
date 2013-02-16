package org.jauntsy.grinder.karma.demo.bolts;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.IOException;
import java.util.List;

import static org.jauntsy.nice.Nice.M;
import static org.jauntsy.nice.Nice.list;
import static us.monoid.web.Resty.content;

/**
 * User: ebishop
 * Date: 2/8/13
 * Time: 2:16 PM
 * <p/>
 * Expects: tuple { $listField: list(tuple { value, label }) }
 * <p/>
 * More precisely, expects a tuple with a single value, that value is a list of pre-sorted tuples
 * of the form value1, value2, valueN, label (The assumption is that the list was sorted by the values)
 */
public class DucksBoardLeaderboardBolt extends DuckBoardBolt {

    private final String listField;
    private final int labelIndex;
    private final int valueIndex;

    public DucksBoardLeaderboardBolt(Fields idFields, String widgetId, String listField, int columnOfLabelInListField, int columnOfValueInListField) {
        super(idFields, widgetId);
        this.listField = listField;
        this.labelIndex = columnOfLabelInListField;
        this.valueIndex = columnOfValueInListField;
    }

    @Override
    protected void updateOne(Tuple input) throws IOException {
        try {
            List<List> raw = (List<List>) input.getValueByField(listField);
            if (raw == null)
                raw = list();
            List items = list();
            for (List row : raw) {
                items.add(M("name", row.get(labelIndex), "values", list(row.get(valueIndex))));
            }
            post(M("value", M("board", items)));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
