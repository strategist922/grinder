package org.jauntsy.grinder.karma.operations.reduce;

import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.karma.mapred.Reducer;
import org.jauntsy.grinder.panama.UniversalComparator;

import java.util.List;
import java.util.TreeSet;

import static org.jauntsy.nice.Nice.*;

/**
 * User: ebishop
 * Date: 1/23/13
 * Time: 11:45 AM
 *
 * Expects as input: a list of tuples (ie. a list of lists of values)
 * Sorts using natural ordering as specified in UniversalComparator
 *
 */
public class TopN extends Reducer {

    private final int n;
    private final boolean ascending;

    public TopN(int n) {
        this(n, false);
    }

    public TopN(int n, boolean ascending) {
        this.n = n;
        this.ascending = ascending;
    }

    @Override
    public List reduce(Tuple key, Tuple a, Tuple b) {
        // *** get the two lists of tuples, put them all in a treeset which sorts them by columns
        TreeSet<List> set = addall(
                new TreeSet<List>(UniversalComparator.LIST_COMPARATOR),
                (List<List>) a.getValue(0),
                (List<List>) b.getValue(0)
        );
        // *** trim the fat
        if (ascending) {
            while (set.size() > n)
                set.remove(set.last());
            return list(flatlist(set.iterator()));
        } else {
            while (set.size() > n)
                set.remove(set.first());
            return list(flatlist(set.descendingIterator()));
        }
    }



}

/*
new Reducer() {
            @Override
            public List reduce(Tuple key, Tuple a, Tuple b) {
                // *** get the two lists of tuples, put them all in a treeset which sorts them by columns
                TreeSet set = addall(new TreeSet(UniversalComparator.LIST_COMPARATOR), (List) a.getValue(0), (List) b.getValue(0));
                // *** trim the fat
                while (set.size() > 4)
                    set.remove(set.first());
                return L(new ArrayList(set));
            }
        }
        */