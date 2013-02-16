package org.jauntsy.grinder.panama.api;

import java.util.List;

/**
 * User: ebishop
 * Date: 12/13/12
 * Time: 1:10 PM
 */
public interface ChangesWriter extends QueueWriter<Change> {

    void putOne(Change change);

    void putBatch(List<Change> batch);

}
