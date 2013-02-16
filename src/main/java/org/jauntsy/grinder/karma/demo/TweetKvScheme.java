package org.jauntsy.grinder.karma.demo;

import backtype.storm.tuple.Fields;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.jauntsy.nice.Nice.flatlist;
import static org.jauntsy.nice.Nice.list;

/**
 * User: ebishop
 * Date: 1/29/13
 * Time: 4:06 PM
 */
public class TweetKvScheme extends KvScheme {

    private static Charset UTF8 = Charset.forName("UTF-8");

    private final List<String> paths;
    private final List<String> fields;

    public TweetKvScheme(List<String> fields, List<String> paths) {
        super(list(fields.get(0)), flatlist(fields.subList(1, fields.size())));
        this.paths = paths;
        this.fields = fields;
        if (!paths.get(0).equals("id_str"))
            throw new IllegalArgumentException("First field MUST map to id_str");
    }

    public static byte[] serialize(JSONObject jo) {
        return jo.toJSONString().getBytes(UTF8);
    }

    @Override
    public List buildRow(byte[] bytes) {
        String jsonString;
        JSONObject jo;
        try {
            jsonString = new String(bytes, UTF8);
            jo = (JSONObject) JSONValue.parse(jsonString);
            if (jo == null)
                return null;
        } catch(Exception ex) {
            ex.printStackTrace();
            return null;
        }

        try {
            List<Object> ret = new ArrayList(fields.size());
            if (jo.containsKey("delete")) {
                JSONObject deleteNode = (JSONObject) jo.get("delete");
                JSONObject statusNode = (JSONObject) deleteNode.get("status");
                String id_str = (String) statusNode.get("id_str");
                ret.add(id_str);
                for (int i = 1; i < fields.size(); i++)
                    ret.add(null);
            } else {
                if (!jo.containsKey("id_str"))
                    return null;
                for (int i = 0; i < paths.size(); i++) {
                    String path = paths.get(i);
                    String[] pathParts = path.split("\\.");
                    Object value = jo;
                    for (String part : pathParts) {
                        if (value != null) {
                            value = ((JSONObject) value).get(part);
                        }
                    }
                    ret.add(value);
                }
            }
            return ret;
        } catch (RuntimeException ex) {
            System.out.println("TweetScheme.deserialize: error processing message: " + jsonString);
            throw ex;
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(fields);
    }

}
