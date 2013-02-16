package org.jauntsy.grinder.karma.demo;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
* User: ebishop
* Date: 1/29/13
* Time: 4:06 PM
*/
public class JsonScheme implements Scheme {

    private static Charset UTF8 = Charset.forName("UTF-8");

    private final Fields fields;

    public JsonScheme(String... fields) {
        this(new Fields(fields));
    }

    public JsonScheme(Fields fields) {
        this.fields = fields;
    }

    public static byte[] serialize(JSONObject jo) {
        return jo.toJSONString().getBytes(UTF8);
    }

    @Override
    public List<Object> deserialize(byte[] bytes) {
        String jsonString = new String(bytes, UTF8);
        JSONObject jo = (JSONObject) JSONValue.parse(jsonString);
        List<Object> ret = new ArrayList(fields.size());
        for (String field : fields) {
            Object value = jo.get(field);
            ret.add(value);
//            if (value == null && !jo.containsKey(field)) { // *** If any fields are undefined
//                return null;    // *** don't send the tuple
//            } else {
//                ret.add(value);
//            }
        }
        return ret;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }

}
