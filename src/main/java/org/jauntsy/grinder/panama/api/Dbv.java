package org.jauntsy.grinder.panama.api;

import org.jauntsy.grinder.panama.UniversalComparator;
import org.jauntsy.nice.Time;
import junit.framework.TestCase;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import scala.actors.threadpool.Arrays;

import java.util.*;

import static junit.framework.Assert.assertEquals;

/**
 * User: ebishop
 * Date: 12/10/12
 * Time: 2:37 PM
 */
public class Dbv {

    private static final Comparator<List> KEY_COMPARATOR = new KeyComparator();

    List _id;
    Map<String,VersionedValue> values = new TreeMap<String,VersionedValue>();
    Map<String,Map<List,Dbv>> relations = new TreeMap<String,Map<List,Dbv>>();

    public Dbv() {
        this._id = null;
    }

    public Dbv(Object... id) {
        this(Arrays.asList(id));
    }

    public Dbv(List id) {
        this._id = id;
    }

    public Dbv(Dbo that, Long version) {
        this(that, version.longValue());
    }

    public Dbv(Dbo that, long version) {
        this._id = that._id;
        for (Map.Entry<String,Object> e : that.columns.entrySet()) {
            this.values.put(e.getKey(), new VersionedValue(e.getValue(), version));
        }
        for (Map.Entry<String,Map<List,Dbo>> e : that.relations.entrySet()) {
            String tableName = e.getKey();
            Map<List, Dbv> table = useTable(tableName);
            for (Map.Entry<List,Dbo> e2 : e.getValue().entrySet()) {
                List id = e2.getKey();
                table.put(id, new Dbv(e2.getValue(), version));
            }
        }
    }

    public Map<String,VersionedValue> getValues() {
        return values;
    }

    public Map<String, Map<List, Dbv>> getRelations() {
        return relations;
    }

    private boolean _put(String column, VersionedValue candidateValue) {
        VersionedValue currentValue = values.get(column);
        if (currentValue == null || candidateValue.compareTo(currentValue) > 0) {
            values.put(column, candidateValue);
            return true;
        } else {
            return false;
        }
    }

    public boolean _put(String column, Object value, long timestamp) {
        VersionedValue candidateValue = new VersionedValue(value, timestamp);
        return _put(column, candidateValue);
    }

    public void put(String column, String value, long timestamp) {
        _put(column, value, timestamp);
    }

    public void put(String column, String value) {
        _put(column, value, now());
    }

    public void put(String column, Number value, long timestamp) {
        _put(column, value, timestamp);
    }

    public void put(String column, Number value) {
        _put(column, value, now());
    }

    public void put(String column, Boolean value, long timestamp) {
        _put(column, value, timestamp);
    }

    public void put(String column, Boolean value) {
        _put(column, value, now());
    }

    public boolean put(String relation, Dbv doc) {
        return useRel(relation, doc._id).merge(doc);
    }

    public void put(String relation, Dbo doc, long timestamp) {
        useRel(relation, doc._id).merge(doc, timestamp);
    }

    public void put(String relation, Dbo doc) {
        put(relation, doc, now());
    }

    public boolean merge(Dbv that) {
        boolean modified = false;
        for (String column : that.values.keySet()) {
            if (_put(column, that.values.get(column)))
                modified = true;
        }
        for (String tableName : that.relations.keySet()) {
            Map<List, Dbv> table = that.relations.get(tableName);
            for (List key : table.keySet()) {
                if (this.useRel(tableName, key).merge(table.get(key)))
                    modified = true;
            }
        }
        return modified;
    }

    public boolean merge(Dbo that, long then) {
        boolean modified = false;
        for (Map.Entry<String,Object> column : that.columns.entrySet()) {
            if (_put(column.getKey(), column.getValue(), then))
                modified = true;
        }
        for (Map.Entry<String,Map<List,Dbo>> relation : that.relations.entrySet()) {
            String tableName = relation.getKey();
            Map<List, Dbo> srcTable = relation.getValue();
            Map<List, Dbv> dstTable = useTable(tableName);
            for (Map.Entry<List,Dbo> row : srcTable.entrySet()) {
                Dbv childDoc = this.useRel(dstTable, row.getKey());
                if (childDoc.merge(row.getValue(), then))
                    modified = true;
            }
        }
        return modified;
    }

    public boolean merge(List values, long then) {
        boolean modified = false;
        Iterator ii = values.iterator();
        while (ii.hasNext()) {
            String key = (String)ii.next();
            Object value = ii.next();
            if ("_id".equals(key)) {
                this._id = (List)value;
            } else if (value instanceof List) {
                for (List row : (List<List>)value) {
                    Dbv child = new Dbv();
                    child.merge(row, then);
                    this.put(key, child);
                }
            } else {
                if (this._put(key, value, then))
                    modified = true;
            }
        }
        return modified;
    }

    private Map<List, Dbv> useTable(String tableName) {
        Map<List, Dbv> table = relations.get(tableName);
        if (table == null) {
            table = new TreeMap<List,Dbv>(KEY_COMPARATOR);
            relations.put(tableName, table);
        }
        return table;
    }

    private Dbv useRel(String tableName, List id) {
        Map<List, Dbv> table = useTable(tableName);
        return useRel(table, id);
    }

    private Dbv useRel(Map<List,Dbv> table, List id) {
        Dbv row = table.get(id);
        if (row == null) {
            row = new Dbv(id);
            table.put(id, row);
        }
        return row;
    }

    private Dbv useRel(String tableName, Object... key) {
        List _id = Arrays.asList(key);
        return useRel(tableName, _id);
    }

    public List getId() {
        return _id;
    }

    public class VersionedValue implements Comparable<VersionedValue> {

        public Object value;
        public long timestamp;

        public VersionedValue(Object value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        @Override
        public int compareTo(VersionedValue that) {
            if (this.timestamp < that.timestamp) return -1;
            else if (this.timestamp > that.timestamp) return 1;
            else return UniversalComparator.INSTANCE.compare(this.value, that.value);
        }

    }

    public static class KeyComparator implements Comparator<List> {

        @Override
        public int compare(List a, List b) {
            return UniversalComparator.INSTANCE.compareLists(a, b);
        }

    }

    private static final long now() {
        return System.currentTimeMillis();
    }




    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb);
        return sb.toString();
    }

    private void toString(StringBuilder sb) {
        if (true) {
        sb.append("(");
        sb.append(":k ").append(JSONValue.toJSONString(_id));
        sb.append(" :v (");
        boolean firstVal = true;
        for (String column : values.keySet()) {
            if (firstVal) firstVal = false; else sb.append(" ");
            sb.append(":").append(column).append(" ");
            sb.append(JSONValue.toJSONString(values.get(column).value));
        }
        for (String tableName : relations.keySet()) {
            Map<List, Dbv> table = relations.get(tableName);
            sb.append(" :").append(tableName).append(" [");
            boolean firstRel = true;
            for (Dbv row : table.values()) {
                if (firstRel) firstRel = false; else sb.append(" ");
                row.toString(sb);
            }

            sb.append("]");
        }
        sb.append(")");
        sb.append(")");
        } else {
            sb.append(this.timestampsAsJson().toJSONString());
        }
    }

    private JSONObject valuesAsJson() {
        return valuesAsJson(false);
    }

    private JSONObject valuesAsJson(boolean includeId) {
        JSONObject jo = new JSONObject();
        if (includeId)
            jo.put("_id", _id);
        for (Map.Entry<String,VersionedValue> e : values.entrySet()) {
            jo.put(e.getKey(), e.getValue().value);
        }
        for (Map.Entry<String,Map<List,Dbv>> e : relations.entrySet()) {
            String tableName = e.getKey();
            Map<List,Dbv> table  = e.getValue();
            List list = new ArrayList();
            for (Dbv r : table.values())
                list.add(r.valuesAsJson(true));
            jo.put(tableName, list);
        }
        return jo;
    }

    private JSONObject toJsonObject() {
        return toJsonObject(false);
    }

    private JSONObject toJsonObject(boolean includeKey) {
        JSONObject jo = new JSONObject();
        if (includeKey)
            jo.put("_id", _id);
        for (Map.Entry<String,VersionedValue> e : values.entrySet()) {
            List v = new ArrayList();
            v.add(e.getValue().value);
            v.add(e.getValue().timestamp);
            jo.put(e.getKey(), v);
        }
        for (Map.Entry<String,Map<List,Dbv>> e : relations.entrySet()) {
            String tableName = e.getKey();
            Map<List,Dbv> table  = e.getValue();
            List list = new ArrayList();
            for (Dbv r : table.values())
                list.add(r.toJsonObject(true));
            jo.put(tableName, list);
        }
        return jo;
    }

    private JSONObject timestampsAsJson() {
        return timestampsAsJson(false);
    }

    private JSONObject timestampsAsJson(boolean includeId) {
        JSONObject jo = new JSONObject();
        if (includeId)
            jo.put("_id", _id);
        for (Map.Entry<String,VersionedValue> e : values.entrySet()) {
            jo.put(e.getKey(), e.getValue().timestamp);
        }
        for (Map.Entry<String,Map<List,Dbv>> e : relations.entrySet()) {
            String tableName = e.getKey();
            Map<List,Dbv> table  = e.getValue();
            List list = new ArrayList();
            for (Dbv r : table.values())
                list.add(r.timestampsAsJson(true));
            jo.put(tableName, list);
        }
        return jo;
    }





    public Object getObject(String column) {
        VersionedValue versionedValue = values.get(column);
        return versionedValue == null ? null : versionedValue.value;
    }

    public String getString(String column) {
        return (String)getObject(column);
    }

    public long getVersion(String column) {
        return values.get(column).timestamp;
    }

    public Number getNumber(String column) {
        return (Number)getObject(column);
    }

    public int getInt(String column) {
        Number n = getNumber(column);
        return n.intValue();
    }

    public static void main2(String[] args) {
        Dbv joe = new Dbv(7);
        Dbo update1 = new Dbo("users", 7) {{
            put("name", "Joe");
            put("age", 27);
            put("addresses", new Dbo("home") {{
                put("city", "San Francisco");
                put("state", "CA");
            }});
        }};
        joe.merge(update1, 1000);
        assertEquals("Joe", joe.getString("name"));

        Dbo update2 = new Dbo(7) {{
            put("name", "Joey");
        }};
        joe.merge(update2, 999);
        assertEquals("Joe", joe.getString("name"));

        joe.merge(update2, 1001);
        assertEquals("Joey", joe.getString("name"));

        joe.merge(new Dbo(7) {{
            put("age", 32);
        }}, 2000);
        assertEquals(32, joe.getInt("age"));

        joe.merge(new Dbo(7) {{
            put("age", 33);
        }}, 2000);
        assertEquals(33, joe.getInt("age"));

        joe.merge(new Dbo(7) {{
            put("age", 32);
        }}, 2000);
        assertEquals(33, joe.getInt("age"));

        System.out.println("joe = " + joe.valuesAsJson());
        System.out.println("joe = " + joe.timestampsAsJson());
        System.out.println("joe = " + joe.toJsonObject());
    }

    public static void main(String[] args) {
        Dbv db = null;
        long start = Time.now();
        long count = 0L;
        for (int i = 0; i < 1000000; i++) {
            db = new Dbv() {{
                put("user", new Dbv(7) {{
                    put("name", "Joe");
                    put("email", "joe@joe.com");
                    put("age", 27);
                    put("addresses", new Dbv("home") {{
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
        System.out.println("db.v = " + db.toDoc().toJsonObject(false));
        System.out.println("db.t = " + db.toTimestamps().toJsonObject(false));
    }

    public Dbo toDoc() {
        Dbo doc = new Dbo(_id);

        for (Map.Entry<String,VersionedValue> vve : values.entrySet())
            doc._put(vve.getKey(), vve.getValue().value);

        for (Map.Entry<String,Map<List,Dbv>> relation : relations.entrySet()) {
            for (Map.Entry<List,Dbv> row : relation.getValue().entrySet()) {
                doc.put(relation.getKey(), row.getValue().toDoc());
            }
        }

        return doc;
    }

    public Dbo toTimestamps() {
        Dbo doc = new Dbo(_id);

        for (Map.Entry<String,VersionedValue> vve : values.entrySet())
            doc._put(vve.getKey(), vve.getValue().timestamp);

        for (Map.Entry<String,Map<List,Dbv>> relation : relations.entrySet()) {
            for (Map.Entry<List,Dbv> row : relation.getValue().entrySet()) {
                doc.put(relation.getKey(), row.getValue().toTimestamps());
            }
        }

        return doc;
    }

    public Dbv getRel(String relation, Object... idElements) {
        Map<List, Dbv> table = relations.get(relation);
        if (table == null) return null;
        List id = Arrays.asList(idElements);
        return table.get(id);
    }

    public Dbv getRel(String relation, List id) {
        Map<List, Dbv> table = relations.get(relation);
        if (table == null) return null;
        return table.get(id);
    }

    public static class Test extends TestCase {

        public void testConstructor() {
            Dbo dbo = new Dbo("acme", 7)
                    .append("name", "Joe Smith")
                    .append("age", 27)
                    .append("addresses", new Dbo("home")
                            .append("city", "San Francisco")
                            .append("state", "CA")
                    );
            Dbv dbv = new Dbv(dbo, 1001);
            assertEquals("Joe Smith", dbv.getString("name"));
            assertEquals(1001L, dbv.getVersion("name"));
            assertEquals(27, dbv.getInt("age"));
            assertEquals(1001L, dbv.getVersion("age"));
            System.out.println(dbv);
            assertEquals("San Francisco", dbv.getRel("addresses", "home").getString("city"));
            assertEquals(1001L, dbv.getRel("addresses", "home").getVersion("city"));
            assertEquals("CA", dbv.getRel("addresses", "home").getString("state"));
            assertEquals(1001L, dbv.getRel("addresses", "home").getVersion("state"));
//            assertEquals("San Francisco", dbv.)
        }

    }

}
