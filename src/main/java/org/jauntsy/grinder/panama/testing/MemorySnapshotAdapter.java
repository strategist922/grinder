package org.jauntsy.grinder.panama.testing;

import org.jauntsy.grinder.panama.api.Dbv;
import org.jauntsy.grinder.panama.UniversalComparator;
import org.jauntsy.grinder.panama.api.SnapshotAdapter;

import java.util.*;

/**
 * User: ebishop
 * Date: 12/7/12
 * Time: 5:42 PM
 */
public class MemorySnapshotAdapter implements SnapshotAdapter {

    private static final Map<String,Map<String,Map<List,Dbv>>> nsToTableToEntryMap = new TreeMap<String,Map<String,Map<List,Dbv>>>();

    private String ns;

    MemorySnapshotAdapter(String ns) {
        this.ns = ns;
    }

    @Override
    public void close() {
    }

    @Override
    public List<Dbv> getBatch(String tableName, List<List> ids) {
        synchronized(nsToTableToEntryMap) {
            Map<List,Dbv> table = getTableOrNull(ns, tableName);
            List<Dbv> rval = new ArrayList<Dbv>();
            for (List id : ids) {
                rval.add(getOne(table, id));
            }
            return rval;
        }
    }

    private Map<String,Map<List, Dbv>> getNsOrNull(String ns) {
        synchronized(nsToTableToEntryMap) {
            return nsToTableToEntryMap.get(ns);
        }
    }

    private Map<String,Map<List, Dbv>> getNsOrCreate(String nsName) {
        synchronized(nsToTableToEntryMap) {
            Map<String, Map<List, Dbv>> ns = nsToTableToEntryMap.get(nsName);
            if (ns == null) {
                ns = new TreeMap<String,Map<List, Dbv>>();
                nsToTableToEntryMap.put(nsName, ns);
            }
            return ns;
        }
    }

    private Map<List, Dbv> getTableOrNull(String ns, String tableName) {
        synchronized(nsToTableToEntryMap) {
            Map<String, Map<List, Dbv>> nsOrNull = getNsOrNull(ns);
            return nsOrNull == null ? null : nsOrNull.get(tableName);
        }
    }

    private Map<List, Dbv> getTableOrCreate(String nsName, String tableName) {
        synchronized(nsToTableToEntryMap) {
            Map<String, Map<List, Dbv>> ns = getNsOrCreate(nsName);
            Map<List, Dbv> table = ns.get(tableName);
            if (table == null) {
                table = new TreeMap<List, Dbv>(UniversalComparator.LIST_COMPARATOR);
                ns.put(tableName, table);
            }
            return table;
        }
    }

    @Override
    public Dbv getOne(String tableName, List<Object> key) {
//        System.out.println("HashMapStateConfig.getOne name: " + name + ", table: " + tableName + ", key: " + key);
        synchronized(nsToTableToEntryMap) {
            return getOne(getTableOrNull(ns, tableName), key);
        }
    }

    private Dbv getOne(Map<List, Dbv> table, List id) {
        if (table == null) return null;
        synchronized(table) {
            return table.get(id);
        }
    }

    @Override
    public void putBatch(String tableName, List<List> ids, List<Dbv> values) {
        synchronized(nsToTableToEntryMap) {
            Map<List, Dbv> table = getTableOrCreate(ns, tableName);
            synchronized (table) {
                Iterator<List> ii = ids.iterator();
                Iterator<Dbv> vv = values.iterator();
                while (ii.hasNext()) {
                    List id = ii.next();
                    Dbv value = vv.next();
                    table.put(id, value);
                }
            }
        }
    }

    @Override
    public void putOne(String table, List id, Dbv value) {
//        System.out.println("HashMapStateConfig.putOne name: " + name + ", table: " + table + ", key: " + id + ", value: " + value);
        synchronized (nsToTableToEntryMap) {
            getTableOrCreate(ns, table).put(id, value);
        }
    }

}
