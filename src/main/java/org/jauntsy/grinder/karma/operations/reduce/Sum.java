package org.jauntsy.grinder.karma.operations.reduce;

import backtype.storm.tuple.Tuple;
import clojure.lang.Numbers;
import org.jauntsy.grinder.karma.mapred.Reducer;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ebishop
 * Date: 1/8/13
 * Time: 3:36 PM
 */
public class Sum extends Reducer {

    @Override
    public List reduce(Tuple key, Tuple a, Tuple b) {
        List ret = new ArrayList(a.size());
        for (int i = 0; i < a.size(); i++)
            ret.add(i, Numbers.add(a.getValue(i), b.getValue(i)));
        return ret;
    }

}
