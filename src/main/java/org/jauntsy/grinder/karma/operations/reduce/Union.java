package org.jauntsy.grinder.karma.operations.reduce;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.jauntsy.grinder.karma.mapred.Reducer;
import org.jauntsy.grinder.karma.mapred.SimpleTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ebishop
 * Date: 2/4/13
 * Time: 2:26 PM
 */
public class Union extends Reducer {

    private final Reducer[] reducers;

    public Union(Reducer... reducers) {
        this.reducers = reducers;
    }

    @Override
    public List reduce(Tuple key, Tuple a, Tuple b) {
        List ret = new ArrayList();
        for (int i = 0; i < reducers.length; i++) {
            if (a.getValue(i) == null) {
                ret.add(b.getValue(i));
            } else if (b.getValue(i) == null) {
                ret.add(a.getValue(i));
            } else {
                Tuple _a = new SimpleTuple(new Fields(a.getFields().get(i)), new Values(a.getValue(i)));
                Tuple _b = new SimpleTuple(new Fields(a.getFields().get(i)), new Values(b.getValue(i)));
                ret.addAll(reducers[i].reduce(key, _a, _b));
            }
        }
        return ret;
    }

}
