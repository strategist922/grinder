package org.jauntsy.grinder.panama.api;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 12/21/12
 * Time: 5:30 PM
 */
public interface QueueWriter<T> extends Serializable {

    void prepare(Map stormConfig);
    void cleanup();

    void putOne(T entry);
    void putBatch(List<T> batch);

}
