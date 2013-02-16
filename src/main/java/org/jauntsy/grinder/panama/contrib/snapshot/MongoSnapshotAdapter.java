package org.jauntsy.grinder.panama.contrib.snapshot;

import org.jauntsy.grinder.panama.api.Dbv;
import org.jauntsy.grinder.panama.api.SnapshotAdapter;
import org.jauntsy.grinder.panama.base.BaseSnapshotAdapter;
import org.jauntsy.grinder.panama.base.BaseSnapshotConfig;
import org.jauntsy.grinder.replay.contrib.storage.mongo.MongoDbProperties;
import org.jauntsy.nice.Time;
import com.mongodb.*;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 12/27/12
 * Time: 7:19 PM
 */
public class MongoSnapshotAdapter extends BaseSnapshotAdapter {

    private final DB db;

    public MongoSnapshotAdapter(DB db) {
        this.db = db;
    }

    @Override
    public void close() {
        db.getMongo().close();
    }

    @Override
    public Dbv getOne(String table, List id) {
        String name = db.getName() + "." + table;
//        System.out.println("[" + Thread.currentThread().getName() + "] MongoSnapshotAdapter.getOne from: " + name + ", id: " + id);
        DBCollection coll = db.getCollection(table);
        return toDbv(coll.findOne(new BasicDBObject("_id", buildMongoId(id))), true);
    }

    @Override
    public List<Dbv> getBatch(String table, List<List> ids) {
        String name = db.getName() + "." + table;
//        System.out.println("[" + Thread.currentThread().getName() + "] MongoSnapshotAdapter.getBatch from: " + name + ", ids: " + ids);
        List<String> mongoIds = new ArrayList<String>();
        for (List id : ids)
            mongoIds.add(buildMongoId(id));
        DBCollection coll = db.getCollection(table);

        Map<String,DBObject> results = new HashMap<String,DBObject>();
//        System.out.println(name + " query start");
        long start = Time.now();
        for (DBObject dbo : coll.find(new BasicDBObject("_id", new BasicDBObject("$in", mongoIds)))) {
            results.put((String)dbo.get("_id"), dbo);
        }
        long dur = Time.since(start);
        if (dur > 1000)
            System.out.println(name + " query end " + Time.since(start));
        List<Dbv> ret = new ArrayList<Dbv>(ids.size());
        for (String mongoId : mongoIds) {
            DBObject dbo = results.get(mongoId);
//            System.out.println("get id: " + mongoId + " dbo: " + dbo);
            ret.add(toDbv(dbo, true));
        }
//        System.out.println("MongoSnapshotAdapter.getBatch: DONE");
        return ret;
    }

    @Override
    public void putBatch(String table, List<List> ids, List<Dbv> values) {
//        System.out.println("[" + Thread.currentThread().getName() + "] MongoSnapshotAdapter.putBatch to: " + db.getName() + "." +  table + ", ids: " + ids + ", values: " + values);
        DBCollection coll = db.getCollection(table);
//        List<DBObject> dbos = new ArrayList<DBObject>();
        for (int i = 0; i < ids.size(); i++) {
            List id = ids.get(i);
            Dbv value = values.get(i);
            BasicDBObject dbo = toDbo(value, true);
            coll.save(dbo, i == ids.size() - 1 ? WriteConcern.NORMAL : WriteConcern.SAFE);
//            dbo.put("_id", buildMongoId(id));
//            dbo.put("__id", id);
//            dbos.add(dbo);
        }
//        coll.insert(dbos, WriteConcern.SAFE);
    }

    private static Dbv toDbv(DBObject dbo, boolean root) {
        if (dbo == null) return null;
        if (root) {
            return toDbv((DBObject)dbo.get("doc"), false);
        } else {
            Dbv dbv = new Dbv((List)dbo.get("_id"));
            for (String column : dbo.keySet()) {
                if (column.startsWith("@")) {
                    String table = column.substring(1);
                    List<DBObject> children = (List<DBObject>) dbo.get(column);
                    for (DBObject child : children)
                        dbv.put(table, toDbv(child, false));
                } else if (column.startsWith("_")) {
                } else {
                    List list = (List)dbo.get(column);
                    dbv._put(column, list.get(0), ((Number)list.get(1)).longValue());
                }
            }
            return dbv;
        }
    }

    private static BasicDBObject toDbo(Dbv dbv, boolean root) {
        BasicDBObject dbo = new BasicDBObject();
        if (root) {
            dbo.put("_id", buildMongoId(dbv.getId()));
            dbo.put("doc", toDbo(dbv, false));
        } else {
            dbo.put("_id", dbv.getId());
            for (Map.Entry<String,Dbv.VersionedValue> e: dbv.getValues().entrySet()) {
                List vv = new ArrayList();
                vv.add(e.getValue().value);
                vv.add(e.getValue().timestamp);
                dbo.put(e.getKey(), vv);
            }
            for (Map.Entry<String,Map<List,Dbv>> e : dbv.getRelations().entrySet()) {
                String table = e.getKey();
                List values = new ArrayList();
                for (Dbv child : e.getValue().values())
                    values.add(toDbo(child, false));
                dbo.put("@" + table, values);
            }
        }
        return dbo;
    }

    private static String buildMongoId(List id) {
        StringBuilder sb = new StringBuilder();
        for (Object part : id)
            sb.append("/").append(String.valueOf(part));
        return sb.toString();
    }

    public static class Config extends BaseSnapshotConfig {

        private final MongoDbProperties mongoConfig;

        public Config(MongoDbProperties mongoConfig) {
            this.mongoConfig = mongoConfig;
        }

        @Override
        public SnapshotAdapter buildStateAdapter(Map stormConfig) {
            try {
                Mongo mongo = mongoConfig.buildMongo(stormConfig);
                DB db = mongo.getDB(mongoConfig.getDb());
                return new MongoSnapshotAdapter(db);
            } catch(Exception ex) {
                throw new RuntimeException(ex);
            }
        }

    }

    public static class Test extends TestCase {
        public void testToDbo() {
            Dbv dbv = new Dbv("acme", 7) {{
                put("name", "Joe");
                put("age", 27);
                put("addresses", new Dbv("home") {{
                    put("city", "San Francisco");
                    put("state", "CA");
                }});
            }};
            BasicDBObject dbo = toDbo(dbv, true);
            System.out.println(dbo);
            System.out.println(toDbv(dbo, true));
            System.out.println(toDbv(dbo, true).toDoc());
        }
    }

}
