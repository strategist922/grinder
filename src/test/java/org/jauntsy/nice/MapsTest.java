package org.jauntsy.nice;

import junit.framework.TestCase;

import java.util.Map;

import static org.jauntsy.nice.Nice.M;

/**
 * Created with IntelliJ IDEA.
 * User: elden
 * Date: 2/16/13
 * Time: 9:13 AM
 * To change this template use File | Settings | File Templates.
 */
public class MapsTest extends TestCase {

    public void testGet() {
        Map m = M("a", "aaa", "c", "ccc");
        assertEquals("aaa", Maps.get(m, "a", null));
        assertEquals("bbb", Maps.get(m, "b", "bbb"));
        assertEquals("ccc", Maps.get(m, "c", "asdf"));
    }

}
