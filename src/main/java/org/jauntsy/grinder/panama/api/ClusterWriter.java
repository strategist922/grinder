package org.jauntsy.grinder.panama.api;

/**
 * User: ebishop
 * Date: 12/11/12
 * Time: 6:36 PM
 *
 */
public interface ClusterWriter extends QueueWriter<Update> {

    void put(String table, Dbo value);

    void put(String table, Dbo value, long timestamp);

    void put(String table, Dbo value, long timestamp, String uuid);

}
