package org.jauntsy.grinder.panama.kafka.scheme;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.jauntsy.grinder.panama.api.Dbo;
import org.jauntsy.grinder.panama.api.Change;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * User: ebishop
 * Date: 12/14/12
 * Time: 11:21 AM
 *
 * TODO: Rename to ChangeScheme
 *
 */
public class ArchiveScheme implements Scheme {

    public byte[] serialize(Change c) {
        try {
            JSONObject jo = new JSONObject();
            jo.put("table", c.table);
            jo.put("key", c.key);
            jo.put("update", c.update);
            jo.put("ts", c.timestamp);
            jo.put("uuid", c.uuid);
            jo.put("snapshot", c.snapshot);
            String jsonString = jo.toJSONString();
            return jsonString.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Object> deserialize(byte[] bytes) {
        try {
            String jsonText = new String(bytes, "UTF-8");
            JSONObject jo = (JSONObject) JSONValue.parse(jsonText);

            String table = (String)jo.get("table");

            JSONArray key = (JSONArray)jo.get("key");

            Dbo update = new Dbo();
            update.merge((JSONObject) jo.get("update"));

            long timestamp = ((Number)jo.get("ts")).longValue();

            String uuid = (String)jo.get("uuid");

            Dbo snapshot = new Dbo();
            snapshot.merge((JSONObject)jo.get("snapshot"));

            return new Values(table, key, update.toList(false), timestamp, uuid, snapshot.toList(false));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("table", "key", "update", "timestamp", "uuid", "snapshot");
    }

}
