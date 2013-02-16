package org.jauntsy.grinder.karma.serialization;

import org.json.simple.JSONValue;

import java.nio.charset.Charset;
import java.util.List;

import static org.jauntsy.nice.Nice.L;

/**
 * User: ebishop
 * Date: 1/8/13
 * Time: 11:17 AM
 */
public class JsonSerializer implements TupleSerializer {

    private final Charset charset;

    public JsonSerializer() {
        this.charset = Charset.forName("UTF-8");
    }

    @Override
    public byte[] serialize(List tuple) {
        return JSONValue.toJSONString(tuple).getBytes(charset);
    }

    @Override
    public List deserialize(byte[] bytes) {
        return (List)JSONValue.parse(new String(bytes, charset));
    }

    public static void main(String[] args) {
        JsonSerializer ser = new JsonSerializer();
        List list = ser.deserialize(ser.serialize(L(L("user", 7), "Joe", 27)));
        System.out.println(list);
    }
}
