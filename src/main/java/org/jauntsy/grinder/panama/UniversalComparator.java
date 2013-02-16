package org.jauntsy.grinder.panama;

import junit.framework.TestCase;
import org.json.simple.JSONValue;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.jauntsy.nice.Nice.L;

/**
 * User: ebishop
 * Date: 11/14/12
 * Time: 4:17 PM
 *
 * TODO: Add support for maps
 *
 */
public class UniversalComparator implements Comparator<Object> {

    public static final UniversalComparator INSTANCE = new UniversalComparator();

    public static final Comparator<List> LIST_COMPARATOR = new Comparator<List>() {
        @Override
        public int compare(List a, List b) {
            return INSTANCE.compareLists(a, b);
        }
    };

    public final int compare(Object _a, Object _b) {
        if (_a == null) return _b == null ? 0 : -1;
        else if (_b == null) return 1;
        else if (_a instanceof Boolean) {
            if (_b instanceof Boolean) {
                return ((Boolean) _a).compareTo((Boolean)_b);
            } else {
                return -1;
            }
        } else if (_a instanceof Number) {
            if (_b instanceof Boolean) {
                return 1;
            } else if (_b instanceof Number) {
                return compareNumbers((Number)_a, (Number)_b);
            } else {
                return -1;
            }
        } else if (_a instanceof String) {
            if (_b instanceof Boolean || _b instanceof Number) {
                return 1;
            } else if (_b instanceof String) {
                return compareStrings((String)_a, (String)_b);
            } else {
                return -1;
            }
        } else if (_a instanceof Object[] || _a instanceof List) {
            if (_b instanceof Boolean || _b instanceof Number || _b instanceof String) {
                return 1;
            } else if (_b instanceof Object[] || _b instanceof List) {
                return compareArraysOrLists(_a, _b);
            } else {
                return -1;
            }
        }
        throw new UnsupportedOperationException("Can't compare '" + _a + "' and '" + _b + "'");
    }

    public final int compareNumbers(Number _a, Number _b) {
        double a = _a.doubleValue();
        double b = _b.doubleValue();
        return a < b ? -1 : a > b ? 1 : 0;
    }

    public final int compareStrings(String a, String b) {
        int lenA = a.length();
        int lenB = b.length();
        for (int i = 0; i < lenA && i < lenB; i++) {
            char ca = a.charAt(i);
            char cb = b.charAt(i);
            if (ca != cb) {
                char la = Character.toLowerCase(ca);
                char lb = Character.toLowerCase(cb);
                if (la < lb) return -1;
                else if (la > lb) return 1;

                if (lenA == i + 1 && lenB == i + 1) {
                    if (ca > cb) return -1;
                    else if (ca < cb) return 1;
                }
            }
        }
        return a.length() < b.length() ? -1 : a.length() > b.length() ? 1 : 0;
    }

    private final int compareArraysOrLists(Object _a, Object _b) {
        if (_a instanceof List) {
            List a = (List)_a;
            List b = _b instanceof List ? (List)_b : Arrays.asList((Object[]) _b);
            return compareLists(a, b);
        } else {
            Object[] a = (Object[])_a;
            Object[] b = _b instanceof List ? ((List)_b).toArray() : ((Object[])_b);
            return compareArrays(a, b);
        }
    }

    public final int compareArrays(Object[] a, Object[] b) {
        int lenA = a.length;
        int lenB = b.length;
        for (int i = 0; i < lenA && i < lenB; i++) {
            int cmp = compare(a[i], b[i]);
            if (cmp != 0) return cmp;
        }
        return a.length < b.length ? -1 : a.length > b.length ? 1 : 0;
    }

    public final int compareLists(List a, List b) {
        if (a == null) {
            return b == null ? 0 : -1;
        } else if (b == null) {
            return 1;
        }
        int lenA = a.size();
        int lenB = b.size();
        for (int i = 0; i < lenA && i < lenB; i++) {
            int cmp = compare(a.get(i), b.get(i));
            if (cmp != 0) return cmp;
        }
        return a.size() < b.size() ? -1 : a.size() > b.size() ? 1 : 0;
    }

    public static class Test extends TestCase {

        public void testAll() {
            Object[] expected = {
                    null, false, true, 1, 2, 3.0, 4, "a", "A", "aa", "AA", "aaa", "AAA", "b", "B", "ba", "bb",
                    new Object[] { "a" },
                    new Object[] { "b" },
                    new Object[] { "b", "c" },
                    new Object[] { "b", "c", "a" },
                    new Object[] { "b", "d" },
                    new Object[] { "b", "d", "e" }
            };
            Object[] shuffled = Arrays.copyOf(expected, expected.length);
            shuffle(shuffled);

            System.out.println("Start");
            UniversalComparator sorter = new UniversalComparator();
            long start = System.currentTimeMillis();
            for (int j = 0; j < 100000; j++) {
                shuffle(shuffled);
                Arrays.sort(shuffled, sorter);
//               for (int i = 0; i < expected.length; i++) {
//                   assertEquals(expected[i], shuffled[i]);
//               }
            }
            long dur = System.currentTimeMillis() - start;
            System.out.println("End: " + dur + " ms");
        }

        private void shuffle(Object[] arr) {
            for (int i = 0; i < arr.length; i++) {
                int d = (int)(Math.random() * arr.length);
                Object tmp = arr[d];
                arr[d] = arr[i];
                arr[i] = tmp;
            }
        }

        public void testCompareNumbers() {
            assertEquals(0, INSTANCE.compareNumbers(0, 0));
            assertEquals(0, INSTANCE.compareNumbers(0, 0.0f));
            assertEquals(0, INSTANCE.compareNumbers(0.0f, 0L));
            assertEquals(-1, INSTANCE.compareNumbers(-1F, 1L));
            assertEquals(-1, INSTANCE.compareNumbers(-1L, 1F));
            assertEquals(1, INSTANCE.compareNumbers(1F, -1L));
            assertEquals(1, INSTANCE.compareNumbers(1L, -1F));
        }

        public void testJson() {
            List v = L("a", 1, 2L, 3.0f, 4.0d, true, L("foo", 7, L("bar", 8, null)));
            assertEquals(0, INSTANCE.compare(v, v));
            assertEquals(0, INSTANCE.compare(v, JSONValue.parse(JSONValue.toJSONString(v))));
        }
    }


}

