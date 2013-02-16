package org.jauntsy.grinder.panama.base;

import org.jauntsy.grinder.panama.api.QueueWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 12/11/12
 * Time: 6:59 PM
 */
public abstract class BaseQueueWriter<T> implements QueueWriter<T> {

    @Override
    public void putOne(T t) {
        List<T> arr = new ArrayList<T>();
        arr.add(t);
        putBatch(arr);
    }

    @Override
    public void prepare(Map stormConfig) {

    }

    @Override
    public void cleanup() {

    }

}
