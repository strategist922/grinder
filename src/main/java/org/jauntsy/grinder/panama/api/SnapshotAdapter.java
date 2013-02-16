package org.jauntsy.grinder.panama.api;

import java.util.List;

/**
 * User: ebishop
 * Date: 12/6/12
 * Time: 5:26 PM
 */
public interface SnapshotAdapter {

    void close();

    List<Dbv> getBatch(String table, List<List> ids);
    Dbv getOne(String table, List<Object> key);

    void putBatch(String table, List<List> ids, List<Dbv> values);
    void putOne(String table, List id, Dbv value);

}
