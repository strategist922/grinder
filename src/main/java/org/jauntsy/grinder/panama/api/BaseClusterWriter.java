package org.jauntsy.grinder.panama.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 12/27/12
 * Time: 5:12 PM
 */
public abstract class BaseClusterWriter implements ClusterWriter {

    @Override
    public void prepare(Map stormConfig) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void putOne(Update entry) {
        List<Update> arr = new ArrayList<Update>();
        arr.add(entry);
        putBatch(arr);
    }

    @Override
    public void put(String table, Dbo value) {
        putOne(new Update(table, value));
    }

    @Override
    public void put(String table, Dbo value, long timestamp) {
        putOne(new Update(table, value, timestamp));
    }

    @Override
    public void put(String table, Dbo value, long timestamp, String uuid) {
        putOne(new Update(table, value, timestamp, uuid));
    }
}
