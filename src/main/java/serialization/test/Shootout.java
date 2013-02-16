package serialization.test;

import org.jauntsy.nice.Time;
import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.msgpack.MessagePack;
import org.msgpack.type.MapValue;
import org.msgpack.type.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * User: ebishop
 * Date: 12/15/12
 * Time: 12:08 PM
 */
public class Shootout {
    public static void main(String[] args) throws Exception {
        Box<Map> bxm = new Box<Map>() {
            @Override
            public Map get() {
                Map m = new HashMap() {{
                    put("_id", 7);
                    put("name", "Joe");
                    put("age", 27);
                    put("addresses", new ArrayList() {{
                        add(new HashMap() {{
                            put("_id", "home");
                            put("city", "San Francisco");
                            put("state", "CA");
                        }});
                    }});
                    put("created_at", System.currentTimeMillis());
                    put("updated_at", System.currentTimeMillis());
                    put("sucks", true);
                    put("someFloat", 3.1414975);
                }};
                return m;
            }
        };
        Box<BSONObject> bxb = new Box<BSONObject>() {
            @Override
            public BSONObject get() {
                BSONObject m = new BasicBSONObject() {{
                    put("_id", 7);
                    put("name", "Joe");
                    put("age", 27);
                    put("addresses", new ArrayList() {{
                        add(new HashMap() {{
                            put("_id", "home");
                            put("city", "San Francisco");
                            put("state", "CA");
                        }});
                    }});
                    put("created_at", System.currentTimeMillis());
                    put("updated_at", System.currentTimeMillis());
                    put("sucks", true);
                    put("someFloat", 3.141975);
                }};
                return m;
            }
        };
        Box<JSONObject> bjo = new Box<JSONObject>() {
            @Override
            public JSONObject get() {
                JSONObject m = new JSONObject() {{
                    put("_id", 7);
                    put("name", "Joe");
                    put("age", 27);
                    put("addresses", new JSONArray() {{
                        add(new JSONValue() {{
                            put("_id", "home");
                            put("city", "San Francisco");
                            put("state", "CA");
                        }});
                    }});
                    put("created_at", System.currentTimeMillis());
                    put("updated_at", System.currentTimeMillis());
                    put("sucks", true);
                    put("someFloat", 3.141975);
                }};
                return m;
            }
        };

        MessagePack pack = new MessagePack();
        Value read = pack.read(pack.write(bxm.get()));
        System.out.println("read = " + read);
//        System.out.println("read.getClass() = " + read.getClass());
//        System.out.println(JSONValue.toJSONString(read));
//
//        System.out.println(JSONValue.toJSONString(m));
        System.out.println("bson: " + new BasicBSONObject(bxm.get()));


//        test("JSONValue  mapToBytesMap", bxm, new SimpleSer());
//        test("JSONValue  joToBytesToJo", bjo, new JSONObjectSer());
        test("msgPack  mapToBytesToMap", bxm, new MsgPackSer());
        test("BSON   bsonToBytesToBson", bxb, new BsonSer());
    }



    private static <T> void test(String label, Box<T> m, Ser<T> ser) throws Exception {
        long start = Time.now();
        byte[] bytes = null;
        for (int i = 0; i < 1000000; i++) {
            bytes = ser.toBytes(m.get());
            ser.fromBytes(bytes);
        }
        System.out.println(label + " ms: " + Time.since(start) + ", size: " + bytes.length);
    }

    static class SimpleSer implements Ser<Map> {
        @Override
        public byte[] toBytes(Map m) throws Exception {
            return JSONValue.toJSONString(m).getBytes("UTF-8");
        }

        @Override
        public Object fromBytes(byte[] bytes) throws Exception {
            return (JSONObject)JSONValue.parse(new String(bytes, "UTF-8"));
        }
    }

    static class MsgPackSer implements Ser<Map> {
        private MessagePack msgPack = new MessagePack();
        @Override
        public byte[] toBytes(Map m) throws Exception {
            return msgPack.write(m);
        }

        @Override
        public Object fromBytes(byte[] bytes) throws Exception {
            MapValue v = (MapValue)msgPack.read(bytes);
//            System.out.println("v = " + v);
            System.out.println("name = " + v.containsKey("name"));
            return v;
        }
    }

    static class JSONObjectSer implements Ser<JSONObject> {
        @Override
        public byte[] toBytes(JSONObject m) throws Exception {
            return m.toJSONString().getBytes("UTF-8");
        }

        @Override
        public Object fromBytes(byte[] bytes) throws Exception {
            return JSONValue.parse(new String(bytes, "UTF-8"));
        }
    }

    public static interface Ser<T> {
        public byte[] toBytes(T m) throws Exception;
        public Object fromBytes(byte[] bytes) throws Exception;
    }

    private static class BsonSer implements Ser<BSONObject> {
        @Override
        public byte[] toBytes(BSONObject m) throws Exception {
            return BSON.encode(m);
        }

        @Override
        public Object fromBytes(byte[] bytes) throws Exception {
            return BSON.decode(bytes);
        }
    }

    static interface Box<T> {
        T get();
    }
}
