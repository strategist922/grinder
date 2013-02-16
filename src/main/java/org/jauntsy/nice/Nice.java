package org.jauntsy.nice;

import java.util.*;

import static junit.framework.Assert.assertEquals;

/**
 * User: ebishop
 * Date: 1/2/13
 * Time: 4:06 PM
 */
public class Nice {

    public static <T> T[] A(T... items) {
        return items;
    }

    public static <T> T[] AO(Class<T> type, T... items) {
        return items;
    }

    public static <T> T[] array(T... items) {
        return items;
    }

    public static <T> T[] arrayof(Class<T> type, T... items) {
        return items;
    }

    public static <T> ArrayList<T> L(T... items) {
        return _fill(new ArrayList<T>(), items);
    }

    public static <T> List<T> LO(Class<T> type, T... items) {
        return _fill(new ArrayList<T>(), items);
    }

    public static <T> List<T> list(T... items) {
        return _fill(new ArrayList<T>(), items);
    }

    public static <T> List<T> flatlist(Collection<T>... listsOfItems) {
        return addall(new ArrayList<T>(), listsOfItems);
    }

    public static <T> List<T> flatlist(Iterator<T>... iteratorsOfItems) {
        return addall(new ArrayList<T>(), iteratorsOfItems);
    }

    public static <T> ArrayList<T> arraylist(T... items) {
        return _fill(new ArrayList<T>(), items);
    }

    public static <T> LinkedList<T> linkedlist(T... items) {
        return _fill(new LinkedList<T>(), items);
    }

    public static <T> List<T> listof(Class<T> type, T... items) {
        return _fill(new ArrayList<T>(), items);
    }

    public static <T> Set<T> S(T... items) {
        return _fill(new HashSet<T>(), items);
    }

    public static <T> Set<T> SO(Class<T> type, T... items) {
        return _fill(new HashSet<T>(), items);
    }

    public static <T> Set<T> hashset(T... items) {
        return _fill(new HashSet<T>(), items);
    }

    public static <T> Set<T> treeset(T... items) {
        return _fill(new TreeSet<T>(), items);
    }

    public static <T> Set<T> treesetby(Comparator<? super T> c, T... items) {
        return _fill(new TreeSet<T>(c), items);
    }


    private static <T,C extends Collection<T>> C _fill(C col, T[] items) {
        for (T t : items)
            col.add(t);
        return col;
    }

    public static Map M(Object... keysAndValues) {
        return _fillmap(new LinkedHashMap(), keysAndValues);
    }

    public static Map map(Object... keysAndValues) {
        return _fillmap(new LinkedHashMap(), keysAndValues);
    }

    public static Map hashmap(Object... keysAndValues) {
        return _fillmap(new HashMap(), keysAndValues);
    }

    public static Map treemap(Object... keysAndValues) {
        return _fillmap(new TreeMap(), keysAndValues);
    }

    public static Map sortedmapby(Comparator c, Object... keysAndValues) {
        return _fillmap(new TreeMap(c), keysAndValues);
    }

    private static Map _fillmap(Map m, Object[] keysAndValues) {
        if (keysAndValues.length % 2 != 0)
            throw new IllegalArgumentException("Only key/value pairs are accepted (ie. an even number of params)");
        for (int i = 0; i < keysAndValues.length; i+=2) {
            m.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return m;
    }

    public static <T> T first(List<T> list) {
        return list.get(0);
    }

    // *** TODO: should rest return empty lists or null? or an iterable?
    public static <T> List<T> rest(List<T> list) {
        return list.size() < 2 ? null : list.subList(1, list.size());
    }

    public static <T> List<T> vector(T... items) {
        List<T> ret = new ArrayList<T>();
        for (T t : items)
            ret.add(t);
        return ret;
    }

    public static <T> List<T> vectorof(Class<T> type, T... items) {
        List<T> ret = new ArrayList<T>();
        for (T t : items)
            ret.add(t);
        return ret;
    }

    public static <T> List<T> cons(T first, Collection<T> rest) {
        List<T> ret = new ArrayList<T>();
        ret.add(first);
        ret.addAll(rest);
        return ret;
    }

    public static <T, S extends Collection<T>, I extends T> S add(S dest, I... sources) {
        for (T t : sources) {
            dest.add(t);
        }
        return dest;
    }

    public static <T, S extends Collection<T>> S addall(S dest, Iterator<? extends T>... sources) {
        for (Iterator<? extends T> c : sources) {
            while (c.hasNext())
                dest.add(c.next());
        }
        return dest;
    }

    public static <T, S extends Collection<T>> S addall(S dest, Collection<? extends T>... sources) {
        for (Collection<? extends T> c : sources) {
            dest.addAll(c);
        }
        return dest;
    }

    public static void main(String[] args) {
        Map joe1 = map("name", "Joe", "age", 27, "addresses", list(map("_id", "home", "city", "San Francisco", "state", "CA")));
        System.out.println("joe1 = " + joe1);
        Map joe2 = M("name", "Joe", "age", 27, "addresses", L(M("city", "San Francisco", "state", "CA"), M("state", "TX")));
        System.out.println("joe2 = " + joe2);

        List abc = Arrays.asList("a", "b", "c");
        assertEquals("a", first(abc));
        assertEquals("b", rest(abc).get(0));
        assertEquals("c", rest(abc).get(1));
        assertEquals(2, rest(abc).size());

        String[] array = array("a");
        assertEquals("a", array[0]);
        array = array("a", "b");
        assertEquals("a", array[0]);
        assertEquals("b", array[1]);

        Number[] numbers = array(2, 3.7f, 44d);

        List<? extends Number> l = L(1, 3.7f, 4.4d);
        List<Number> lo = LO(Number.class, 1, 3.7f, 4.4d);
    }

    static public class Test {

        public static void main(String[] args) {
            System.out.println(sum(list(1, 2, 3, 4, 5)));
            L((Object)null);
        }

        public static int sum(List<Integer> values) {
            return values == null ? 0 : first(values) + sum(rest(values));
        }
    }

}
