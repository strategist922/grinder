package org.jauntsy.grinder.panama;

import backtype.storm.tuple.Values;
import org.jauntsy.grinder.panama.api.Dbo;
import org.jauntsy.nice.Time;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.List;

/**
 * User: ebishop
 * Date: 12/11/12
 * Time: 1:00 PM
 */
public class SpeedTest {
    public static void main(String[] args) throws Exception {
//        a();
//        b();
//        c();
        System.out.println(new Dbo(7) {{
            put("name", "joe");
            put("age", 27);
            put("addresses", new Dbo("home") {{
                put("city", "San Francisco");
                put("state", "CA");
            }});
        }});
    }

    private static void a() {
        Dbo doc = null;
        long count = 0L;
        long start = Time.now();
        for (int i = 0; i < 1000000; i++) {
            doc = new Dbo();
            JSONObject parse = (JSONObject)JSONValue.parse("{\"_id\":[\"user\",7],\"name\":\"Joe\",\"age\":27,\"color\":\"green\"}");
            doc.merge(parse);
            count++;
        }
        long delta = Time.since(start);
        long qps = 1000L * count / delta;
        System.out.println("a qps = " + qps);
        System.out.println("doc = " + doc);
    }

    private static void b() {
        Dbo doc = null;
        long count = 0L;
        long start = Time.now();
        for (int i = 0; i < 1000000; i++) {
            doc = new Dbo();
//            List parse = (JSONObject)JSONValue.parse("{\"_id\":[\"user\",7],\"name\":\"Joe\",\"age\":27,\"color\":\"green\"}");
            List parse = new Values("_id", new Values("user", 7), "name", "Joe", "age", 27, "color", "green");
            doc.merge(parse);
            count++;
        }
        long delta = Time.since(start);
        long qps = 1000L * count / delta;
        System.out.println("b qps = " + qps);
        System.out.println("doc = " + doc);
    }

    private static void c() {
        Dbo doc = null;
        long count = 0L;
        long start = Time.now();
        for (int i = 0; i < 1000000; i++) {
            doc = Dbo.fromList(
                    new Values(
                            "_id", new Values("user", 7),
                            "name", "Joe",
                            "age", 27,
                            "color", "green",
                            "addresses", new Values(
                            new Values(
                                    "_id", new Values("home"),
                                    "city", "SF",
                                    "state", "CA"
                            ),
                            new Values(
                                    "_id", new Values("work"),
                                    "city", "Mountain View",
                                    "state", "CA"
                            )
                    )
                    )
            );
            count++;
        }
        long delta = Time.since(start);
        long qps = 1000L * count / delta;
        System.out.println("b qps = " + qps);
        System.out.println("doc.c = " + doc);
        System.out.println("doc.c.toList = " + doc.toList());
    }


}
