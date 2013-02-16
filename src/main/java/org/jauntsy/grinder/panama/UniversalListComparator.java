package org.jauntsy.grinder.panama;

import java.util.Comparator;
import java.util.List;

/**
 * User: ebishop
 * Date: 1/22/13
 * Time: 9:43 AM
 */
public class UniversalListComparator implements Comparator<List> {

    public static final UniversalListComparator INSTANCE = new UniversalListComparator();

    private static final UniversalComparator impl = UniversalComparator.INSTANCE;

    @Override
    public int compare(List a, List b) {
        return impl.compareLists(a, b);
    }

}
