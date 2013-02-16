package org.jauntsy.grinder.panama.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 12/14/12
 * Time: 4:27 PM
 */
public class Sweetie {

    public static <T> List<T> list(T... items) {
        List<T> l = new ArrayList<T>();
        for (T t : items)
            l.add(t);
        return l;
    }

    public static Map map(Object... more) {
        if (more.length % 2 != 0)
            throw new IllegalArgumentException("Only key/value pairs are accepted (ie. an even number of params)");
        Map m = new HashMap();
        for (int i = 0; i < more.length; i+=2) {
            m.put(more[i], more[i + 1]);
        }
        return m;
    }

    public static void main(String[] args) {
        System.out.println(map("name", "Joe", "age", 27, "colors", list("green", "red", "yellow")));
        Map<String, Integer> foo = map("foo", 8);
    }

}
