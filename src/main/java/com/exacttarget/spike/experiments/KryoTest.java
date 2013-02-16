package com.exacttarget.spike.experiments;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.jauntsy.nice.Time;
import com.mongodb.BasicDBList;
import com.mongodb.DefaultDBDecoder;
import com.mongodb.DefaultDBEncoder;
import org.json.simple.JSONValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static org.jauntsy.nice.Nice.*;

/**
 * User: ebishop
 * Date: 1/8/13
 * Time: 9:59 AM
 *
 * Kryo is the serialization library used by Storm
 *
 */
public class KryoTest {

    static private Kryo kryo = new Kryo();

    public static void main(String[] args) throws IOException {
        List tuple = L(L("hello"), L("user", 7), M("count", 1L));

        System.out.println("json: " + fromJsonBytes(toJsonBytes(tuple)));
        System.out.println("json: size: " + toJsonBytes(tuple).length);

        System.out.println("kryo: " + pingpong(tuple, ArrayList.class));
        System.out.println("kryo: size: " + toBytes(tuple).length);

        BasicDBList bson = new BasicDBList();
        bson.addAll(tuple);
        byte[] bsonBytes = bsonEncoder.encode(bson);
        System.out.println("bson: " + bsonDecoder.readObject(bsonBytes));
        System.out.println("bson: size: " + bsonBytes.length);

        System.out.println("Speed test: be patient");
        long loops = 1000000;
        testJson(loops, tuple);
        testKryo(loops, tuple);
        testBson(loops, tuple);

    }

    private static DefaultDBEncoder bsonEncoder = new DefaultDBEncoder();
    private static DefaultDBDecoder bsonDecoder = new DefaultDBDecoder();

    private static void testBson(long loops, List tuple) {
        long start = Time.now();
        for (int i = 0; i < loops; i++) {
            BasicDBList list = new BasicDBList();
            list.addAll(tuple);
            byte[] bytes = bsonEncoder.encode(list);
            Object o = bsonDecoder.readObject(bytes);
        }
        long qps = 1000L * loops / Time.since(start);
        System.out.println("bson qps: " + qps);
    }

    private static void testKryo(long loops, List tuple) {
        long start = Time.now();
        for (int i = 0; i < loops; i++)
            pingpong(tuple, ArrayList.class);
        long qps = 1000L * loops / Time.since(start);
        System.out.println("kryo qps: " + qps);
    }

    private static void testJson(long loops, List tuple) {
        long start = Time.now();
        for (int i = 0; i < loops; i++)
            fromJsonBytes(toJsonBytes(tuple));
        long qps = 1000L * loops / Time.since(start);
        System.out.println("json qps: " + qps);
    }

    private static <T> T pingpong(Object o, Class<T> type) {
        byte[] bytes = toBytes(o);
        return fromBytes(bytes, type);
    }

    private static byte[] toJsonBytes(Object o) {
        try {
            return JSONValue.toJSONString(o).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object fromJsonBytes(byte[] bytes) {
        try {
            return JSONValue.parse(new String(bytes, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] toBytes(Object o) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Output out = new Output(bytes);
        kryo.writeObject(out, o);
        out.close();
        return bytes.toByteArray();
    }

    private static <T> T fromBytes(byte[] bytes, Class<T> type) {
        final Input input = new Input(bytes);
        return kryo.readObject(input, type);
    }
}
