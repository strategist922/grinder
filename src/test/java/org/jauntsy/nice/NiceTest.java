package org.jauntsy.nice;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.jauntsy.grinder.panama.UniversalComparator;

import java.util.*;

import static org.jauntsy.nice.Nice.*;

/**
 * Created with IntelliJ IDEA.
 * User: elden
 * Date: 2/16/13
 * Time: 9:01 AM
 * To change this template use File | Settings | File Templates.
 */
public class NiceTest extends TestCase {

    public void testList() {
        List<String> list = list("a", "b", "c");
        assertEquals(3, list.size());
        assertEquals("a", list.get(0));
        assertEquals("b", list.get(1));
        assertEquals("c", list.get(2));
        assertTrue(list instanceof ArrayList);

        list = listof(String.class);
        assertEquals(0, list.size());
        assertTrue(list instanceof ArrayList);

        list = arraylist("a", "b", "c");
        assertEquals(3, list.size());
        assertEquals("a", list.get(0));
        assertEquals("b", list.get(1));
        assertEquals("c", list.get(2));
        assertTrue(list instanceof ArrayList);

        list = linkedlist("a", "b", "c");
        assertEquals(3, list.size());
        assertEquals("a", list.get(0));
        assertEquals("b", list.get(1));
        assertEquals("c", list.get(2));
        assertTrue(list instanceof LinkedList);
    }

    public void testMap() {
        Map m = M("a", "aaa", "b", 123, "c", list("a", "b", "c"));
        assertEquals(3, m.size());
        assertEquals("aaa", m.get("a"));
        assertEquals(123, m.get("b"));
        assertEquals(list("a", "b", "c"), m.get("c"), UniversalComparator.LIST_COMPARATOR);
        Iterator<Map.Entry> itor = m.entrySet().iterator();
        assertEquals("aaa", itor.next().getValue());
        assertEquals(123, itor.next().getValue());
        assertEquals(list("a", "b", "c"), itor.next().getValue(), UniversalComparator.LIST_COMPARATOR);
    }

    private void assertEquals(Object expected, Object actual, Comparator comparator) {
        if (comparator.compare(expected, actual) != 0) {
            failNotEquals(null, expected, actual);
        }
    }

}
