package org.jauntsy.grinder.panama.api;

import org.jauntsy.grinder.panama.UniversalComparator;
import org.jauntsy.nice.Time;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import scala.actors.threadpool.Arrays;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.*;

/**
 * User: ebishop
 * Date: 12/10/12
 * Time: 4:38 PM
 */
public class Dbo {

    List _id;

    TreeMap<String, Object> columns;
    TreeMap<String, Map<List, Dbo>> relations;

    public Dbo() {
        this((List)null);
    }

    public Dbo(List _id) {
        this._id = _id;
        this.columns = new TreeMap<String, Object>();
        this.relations = new TreeMap<String, Map<List,Dbo>>();
    }

    public Dbo(Object... _id) {
        this(Arrays.asList(_id));
    }

    void _put(String column, Object value) {
        this.columns.put(column, value);
    }

    public void put(String column, String value) {
        _put(column, value);
    }

    public void put(String column, Number value) {
        _put(column, value);
    }

    public void put(String column, Boolean value) {
        _put(column, value);
    }

    public void put(String tableName, Dbo relation) {
        Map<List,Dbo> table = this.relations.get(tableName);
        if (table == null) {
            table = new TreeMap<List,Dbo>(UniversalComparator.LIST_COMPARATOR);
            this.relations.put(tableName, table);
        }
        table.put(relation._id, relation);
    }

    public String toString() {
        return toJsonObject().toString();
    }

    public JSONObject toJsonObject() {
        return toJsonObject(true);
    }

    public JSONObject toJsonObject(boolean includeId) {
        JSONObject json = new JSONObject();

        if (includeId)
            json.put("_id", _id);

        for (Map.Entry<String,Object> e : columns.entrySet()) {
            json.put(e.getKey(), e.getValue());
        }

        for (Map.Entry<String,Map<List,Dbo>> e : relations.entrySet()) {
            String tableName = e.getKey();
            Map<List, Dbo> table = e.getValue();
            List asList = new ArrayList();
            for (Dbo childRow : table.values())
                asList.add(childRow.toJsonObject(true));
            json.put(tableName, asList);
        }

        return json;
    }

    public String toJsonString(boolean includeId) {
        try {
            StringWriter w = new StringWriter();
            toJsonString(includeId, w);
            return w.toString();
        } catch(IOException neverGonnaHappen) {
            throw new RuntimeException(neverGonnaHappen);
        }
    }

    public void toJsonString(boolean includeId, Writer w) throws IOException {
        w.append('{');

        boolean first = true;

        if (includeId) {
            w.append("\"_id\":");
            w.append(JSONValue.toJSONString(_id));
            first = false;
        }

        for (Map.Entry<String,Object> e : columns.entrySet()) {
            if (first) first = false; else w.append(",");
            w.append(JSONValue.toJSONString(e.getKey()));
            w.append(':');
            w.append(JSONValue.toJSONString(e.getValue()));
        }

        for (Map.Entry<String,Map<List,Dbo>> e : relations.entrySet()) {
            String tableName = e.getKey();
            w.append(tableName);
            w.append(':');
            w.append('[');
            Map<List, Dbo> table = e.getValue();
            boolean firstChild = true;
            for (Dbo childRow : table.values()) {
                if (firstChild) firstChild = false; else w.append(',');
                childRow.toJsonString(true, w);
            }
            w.append(']');
        }

        w.append('}');
    }

    public static void main(String[] args) {
        Dbo db = null;
        long start = Time.now();
        long count = 0L;
        for (int i = 0; i < 10000000; i++) {
            db = new Dbo((String)null) {{
                put("user", new Dbo(7) {{
                    put("name", "Joe");
                    put("email", "joe@joe.com");
                    put("age", 27);
                    put("addresses", new Dbo("home") {{
                        put("city", "San Francisco");
                        put("state", "CA");
                    }});
                }});
            }};
            count++;
        }
        long delta = Time.since(start);
        System.out.println("delta = " + delta);
        long qps = 1000L * count / delta;
        System.out.println("qps = " + qps);
        System.out.println("db = " + db.toJsonObject(false));
    }

    public void merge(JSONObject json) {
        //To change body of implemented methods use File | Settings | File Templates.
        for (Object _column : json.keySet()) {
            String columnName = (String)_column;
            Object columnValue = json.get(columnName);
            if (columnName.equals("_id")) {
                this._id = (List)columnValue;
            } else if (columnValue instanceof List) {
                for (Object _childRow : (List)columnValue) {
                    Dbo childDoc = new Dbo();
                    childDoc.merge((JSONObject)_childRow);
                    this.put(columnName, childDoc);
                }
            } else {
                this._put(columnName, columnValue);
            }
        }
    }

    public void merge(List items) {
        Iterator ii = items.iterator();
        while (ii.hasNext()) {
            String column = (String)ii.next();
            Object value = ii.next();
            if (column.equals("_id")) {
                this._id = (List)value;
            } else if (value instanceof List) {
                for (Object _childRow : (List)value) {
                    Dbo childDoc = new Dbo();
                    childDoc.merge((List)_childRow);
                    this.put(column, childDoc);
                }
            } else {
                this._put(column, value);
            }
        }
    }

    public List toList() {
        return toList(true);
    }

    public List toList(boolean includeId) {
        List rval = new ArrayList();
        if (includeId) {
            rval.add("_id");
            rval.add(_id);
        }
        for (Map.Entry<String,Object> column : columns.entrySet()) {
            rval.add(column.getKey());
            rval.add(column.getValue());
        }
        for (Map.Entry<String,Map<List,Dbo>> relation : relations.entrySet()) {
            String columnName = relation.getKey();
            List childRows = new ArrayList();
            for (Map.Entry<List,Dbo> row : relation.getValue().entrySet()) {
                childRows.add(row.getValue().toList());
            }
            rval.add(columnName);
            rval.add(childRows);
        }
        return rval;
    }

    public static Dbo fromList(List values) {
        Dbo doc = new Dbo();
        doc.merge(values);
        return doc;
    }

    public Dbo append(String column, Number value) {
        _put(column, value);
        return this;
    }

    public Dbo append(String column, String value) {
        _put(column, value);
        return this;
    }

    public Dbo append(String column, boolean value) {
        _put(column, value);
        return this;
    }

    public Dbo append(String column, Dbo childRow) {
        put(column, childRow);
        return this;
    }

    public List getId() {
        return _id;
    }

    public String getString(String column) {
        return (String)columns.get(column);
    }

    public byte getByte(String column) {
        return getNumber(column).byteValue();
    }

    public short getShort(String column) {
        return getNumber(column).shortValue();
    }

    public int getInteger(String column) {
        return getNumber(column).intValue();
    }

    public long getLong(String column) {
        return getNumber(column).longValue();
    }

    public float getFloat(String column) {
        return getNumber(column).floatValue();
    }

    public double getDouble(String column) {
        return getNumber(column).doubleValue();
    }

    public Long getLongOrNull(String column) {
        Number n = getNumber(column);
        return n == null ? null : n instanceof Long ? (Long)n : n.longValue();
    }

    private Number getNumber(String column) {
        return (Number)columns.get(column);
    }
}
