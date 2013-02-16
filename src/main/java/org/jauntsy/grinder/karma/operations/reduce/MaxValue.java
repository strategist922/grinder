package org.jauntsy.grinder.karma.operations.reduce;

import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.karma.mapred.Reducer;
import org.jauntsy.grinder.panama.UniversalComparator;

import java.util.List;

/**
 * User: ebishop
 * Date: 1/15/13
 * Time: 5:39 PM
 *
 * Reduces to a single value by picking the smaller value when presented with two
 *
 */
public class MaxValue extends Reducer {
    @Override
    public List reduce(Tuple key, Tuple a, Tuple b) {
        return pickMax(a.getValues(), b.getValues());
    }

    private List pickMax(List a, List b) {
        return UniversalComparator.LIST_COMPARATOR.compare(a, b) >= 0 ? a : b;
    }
}
