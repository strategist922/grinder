package org.jauntsy.grinder.panama.model;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

import java.util.List;

/**
 * User: ebishop
 * Date: 12/31/12
 * Time: 4:36 PM
 */
public class BsonDbv {

    BSONObject values;
    BSONObject timestamps;

    public BsonDbv(Dbo dbo, long timestamp) {
        this.values = ((BsonDbo)dbo).data;
        this.timestamps = new BasicBSONObject();
        _updateTimestamps(values, timestamps, timestamp);
    }

    private static void _updateTimestamps(BSONObject values, BSONObject timestamps, long timestamp) {
        for (String s : values.keySet()) {
            if (s.startsWith("_")) {
                // ignore
            } else if (s.startsWith("@")) {
                _updateRows(values, timestamps, s, timestamp);
            } else {
                timestamps.put(s, timestamp);
            }
        }
    }

    private static void _updateRows(BSONObject values, BSONObject timestamps, String tableKey, long timestamp) {
        List<BSONObject> valueRows = (List<BSONObject>)values.get(tableKey);
        List timestampRows = new BasicBSONList();
        for (int i = 0; i < valueRows.size(); i++) {
            BSONObject valueRow = valueRows.get(i);
            BSONObject timeRow = new BasicBSONObject();
            timeRow.put("_id", valueRow.get("_id"));
            _updateTimestamps(valueRow, timeRow, timestamp);
            timestampRows.add(timeRow);
        }
        timestamps.put(tableKey, timestampRows);
    }

    public BsonDbv append(String column, String value, long timestamp) {
        values.put(column , value);
        timestamps.put(column, timestamp);
        return this;
    }

    public static void main(String[] args) {
        BsonDbo joe = new BsonDbo("acme", 7) {{
            append("name", "Joe");
            append("age", 27);
            append("addresses", new BsonDbo("home") {{
                append("city", "San Francisco");
                append("state", "CA");
            }});
        }};

        BsonDbv dbv = new BsonDbv(joe, 1234);
        System.out.println(dbv.values);
        System.out.println(dbv.timestamps);
    }

}
