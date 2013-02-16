package org.jauntsy.nice;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: elden
 * Date: 2/16/13
 * Time: 8:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class Maps {

    public static <K,V> V get(Map<K, V> map, K key, V defaultValue) {
        V ret = map.get(key);
        return ret == null ? defaultValue : ret;
    }

}
