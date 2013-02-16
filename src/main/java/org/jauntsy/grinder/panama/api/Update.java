package org.jauntsy.grinder.panama.api;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * User: ebishop
 * Date: 12/10/12
 * Time: 11:58 AM
 */
public class Update implements Serializable {

    public final String table;
    public final Dbo value;
    public final long timestamp;
    public final String uuid;

    public Update(String table, Dbo value) {
        this(table, value, System.currentTimeMillis());
    }

    public Update(String table, Dbo value, long timestamp) {
        this(table, value, timestamp, UUID.randomUUID().toString());
    }

    public Update(String table, Dbo value, long timestamp, String uuid) {
        this.table = table;
        this.value = value;
        this.timestamp = timestamp;
        this.uuid = uuid;
    }

    public static List<Object> newUpdate(String table, List<Object> key, Object value) {
        return new Values(table, key, value, System.currentTimeMillis(), UUID.randomUUID().toString());
    }

    public static List<Object> newUpdate(String table, List<Object> key, Object value, long timestamp) {
        return new Values(table, key, value, timestamp, UUID.randomUUID().toString());
    }

    public static String getTable(Tuple tuple) {
        return tuple.getString(0);
    }

    public static List getKey(Tuple tuple) {
        return (List)tuple.getValue(1);
    }

    public static Object getChange(Tuple tuple) {
        return tuple.getValue(2);
    }

    public static long getTimestamp(Tuple tuple) {
        return tuple.getLong(3);
    }

    public static String getUuid(Tuple tuple) {
        return tuple.getString(4);
    }

    public static Dbo getChangeAsDoc(List id, Tuple tuple) {
        Dbo doc = new Dbo(id);
        Object change = getChange(tuple);
        if (change instanceof JSONObject) {
            doc.merge((JSONObject) change);
        } else if (change instanceof String) {
            doc.merge((JSONObject)JSONValue.parse((String) change));
        } else {
            doc.merge((List) change);
        }
        return doc;
    }

    private static void mergeRow(Dbo doc, List change) {
        Iterator ii = change.iterator();
        while (ii.hasNext()) {
            String column = (String)ii.next();
            Object value = ii.next();
            if (column.equals("_id")) {
                doc._id = (List)value;
            } else if (value instanceof List) {
                List<List> rows = (List<List>)value;
                for (List row : rows) {
                    Dbo rowDoc = new Dbo();
                    mergeRow(rowDoc, row);
                    doc.put(column, rowDoc);
                }
            } else {
                doc._put(column, value);
            }
        }
    }
}
