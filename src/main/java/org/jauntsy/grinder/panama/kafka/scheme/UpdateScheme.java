package org.jauntsy.grinder.panama.kafka.scheme;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.jauntsy.grinder.panama.api.Dbo;
import org.jauntsy.grinder.panama.api.Update;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * User: ebishop
 * Date: 12/14/12
 * Time: 11:21 AM
 */
public class UpdateScheme implements Scheme {

    public byte[] serialize(Update update) {
        try {
            JSONObject jo = new JSONObject();
            jo.put("table", update.table);
            jo.put("key", update.value.getId());
            jo.put("value", update.value.toJsonObject(false));
            jo.put("ts", update.timestamp);
            jo.put("uuid", update.uuid);
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
            JSONObject changes = (JSONObject)jo.get("value");
            Dbo doc = new Dbo();
            doc.merge(changes);
            long timestamp = ((Number)jo.get("ts")).longValue();
            String uuid = (String)jo.get("uuid");
            return new Values(table, key, doc.toList(false), timestamp, uuid);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("table", "key", "changes", "timestamp", "uuid");
    }

}
