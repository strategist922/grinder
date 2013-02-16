package org.jauntsy.grinder.karma.operations.map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.karma.Emitter;
import org.jauntsy.grinder.karma.mapred.Mapper;

import java.util.List;

import static org.jauntsy.nice.Nice.L;

/**
 * User: ebishop
 * Date: 1/23/13
 * Time: 11:53 AM
 *
 * Emits a tuple with one column; a list containing the selected fields.
 * The TopN reducer maintains a list of the top n values; this is why we return a list of one tuple which is
 * merged with another list and then trimmed to fit the size indicated.
 */
public class TopNMapper extends Mapper {

    private final Fields fields;

    public TopNMapper() {
        this.fields = null;
    }

    public TopNMapper(String... fields) {
        this(new Fields(fields));
    }

    public TopNMapper(List<String> fields) {
        this(new Fields(fields));
    }

    public TopNMapper(Fields fields) {
        this.fields = fields;
    }

    @Override
    public void map(Tuple doc, Emitter e) {
        if (fields == null)
            e.emit(L(doc.getValues()));
        else
            e.emit(doc.select(fields));
    }

}
