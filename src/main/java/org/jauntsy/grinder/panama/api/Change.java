package org.jauntsy.grinder.panama.api;

import org.json.simple.JSONObject;

import java.util.List;

/**
 * User: ebishop
 * Date: 12/21/12
 * Time: 5:31 PM
 */
public class Change {

    public String table;
    public List key;
    public JSONObject update;
    public long timestamp;
    public String uuid;
    public JSONObject snapshot;

    public Change(String table, List key, JSONObject update, long timestamp, String uuid, JSONObject snapshot) {
        this.table = table;
        this.key = key;
        this.update = update;
        this.timestamp = timestamp;
        this.uuid = uuid;
        this.snapshot = snapshot;
    }

    public String getTable() {
        return table;
    }

    public List getKey() {
        return key;
    }

    public JSONObject getUpdate() {
        return update;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getUuid() {
        return uuid;
    }

    public JSONObject getSnapshot() {
        return snapshot;
    }

}
