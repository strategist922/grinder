package org.jauntsy.grinder.karma.operations.map;

import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.karma.Emitter;
import org.jauntsy.grinder.karma.mapred.Mapper;

/**
 * User: ebishop
 * Date: 1/15/13
 * Time: 5:38 PM
 */
public class SelectAll extends Mapper {
    @Override
    public void map(Tuple doc, Emitter e) {
        e.emitAll(doc.getValues());
    }
}
