package org.jauntsy.grinder.panama.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.panama.UniversalComparator;
import org.jauntsy.grinder.panama.api.*;
import org.jauntsy.nice.Time;

import java.util.*;

/**
 * User: ebishop
 * Date: 12/7/12
 * Time: 4:23 PM
 *
 * Accepts: uuid, topic, key, timestamp, update
 * eg. ['1234-asdf-1234-asdf', 'user', [7], 987654321, {"name":"Joe", "age":27}]
 *
 *
 */
public class CuratorBolt4 implements IRichBolt {

    private final static long FLUSH_INTERVAL_MS = 200;
    private final static long FLUSH_MAX_ENTRIES = 500;

    private SnapshotConfig stateFactory;
    private ChangesWriter changesArchive;

    private transient TopologyContext context;
    private transient OutputCollector collector;

    private transient long lastFlush;
    private transient SnapshotAdapter state;
    private transient List<Tuple> todo;
//    private transient List<Pending> pending;
//    private transient Map<String,Map<List,Dbv>> latest;
    private transient Thread flushThread;
    private transient Object lock;

    public CuratorBolt4(SnapshotConfig stateFactory, ChangesWriter changesArchive) {
        this.stateFactory = stateFactory;
        this.changesArchive = changesArchive;
    }

    @Override
    public void prepare(Map stormConfig, TopologyContext context, OutputCollector collector) {
        this.context = context;
        this.collector = collector;
        this.lastFlush = System.currentTimeMillis();
        this.state = stateFactory.buildStateAdapter(stormConfig);
        this.changesArchive.prepare(stormConfig);
        this.todo = new ArrayList<Tuple>();
//        this.latest = new HashMap<String,Map<List,Dbv>>();
        this.lock = new Object();
        this.flushThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    List<Tuple> batch = null;
                    synchronized(lock) {
                        while (batch == null) {
                            if (todo.size() > 0) {
                                batch = todo;
                                todo = new ArrayList<Tuple>();
                            } else {
                                try {
                                    lock.wait(200);
                                } catch (InterruptedException expected) {

                                }
                            }
                        }
                    }
                    flushTodo(batch);
                }
            }
        });
        flushThread.start();
    }

    @Override
    public void execute(Tuple tuple) {
        synchronized (lock) {
            todo.add(tuple);
            lock.notify();
        }
    }

    private void flushTodo(List<Tuple> todo) {
        if (todo.size() < 1)
            return;

            // *** build a map of table->ids
            List<Pending> pending = new ArrayList<Pending>();
            Map<String,Set<List>> idsByTable = new HashMap<String,Set<List>>();
            for (Tuple tuple : todo) {
                String table = Update.getTable(tuple);
                Set<List> ids = idsByTable.get(table);
                if (ids == null) {
                    ids = new TreeSet<List>(UniversalComparator.LIST_COMPARATOR);
                    idsByTable.put(table, ids);
                }
                ids.add(Update.getKey(tuple));
            }

            // *** load the latest snapshot of each id
            Map<String,Map<List,Dbv>> latest = new HashMap<String,Map<List,Dbv>>(); // *** TODO: This can also be a cache, let it live
            for (Map.Entry<String,Set<List>> e : idsByTable.entrySet()) {
                String table = e.getKey();
                Set<List> ids = e.getValue();
                List<Dbv> batch = state.getBatch(table, new ArrayList(ids));
                Map<List, Dbv> idToDbvMap = latest.get(table);
                if (idToDbvMap == null) {
                    idToDbvMap = new TreeMap<List, Dbv>(UniversalComparator.LIST_COMPARATOR);
                    latest.put(table, idToDbvMap);
                }
                for (Dbv dbv : batch) {
                    if (dbv != null) {
                        idToDbvMap.put(dbv.getId(), dbv);
                    }
                }
            }

            for (Tuple tuple : todo) {
                String table = Update.getTable(tuple);
                List key = Update.getKey(tuple);
                List changes = (List)Update.getChange(tuple);
                long timestamp = Update.getTimestamp(tuple);
                String uuid = Update.getUuid(tuple);

                Map<List, Dbv> tableLatest = latest.get(table);
                if (tableLatest == null) {
                    tableLatest = new TreeMap<List,Dbv>(UniversalComparator.LIST_COMPARATOR);
                    latest.put(table, tableLatest);
                }
                Dbv currentValue = tableLatest.get(key);
//                if (currentValue == null) {
//                    currentValue = state.getOne(table, key);
//                }
                if (currentValue == null || currentValue.merge(changes, timestamp)) {
                    if (currentValue == null) {
                        currentValue = new Dbv(key);
                        currentValue.merge(changes, timestamp);
                    }
                    Pending next = new Pending(
                            tuple, table, key, currentValue,
                            new Change(table, key, Dbo.fromList(changes).toJsonObject(false), timestamp, uuid, currentValue.toDoc().toJsonObject(false))
                    );
                    pending.add(next);
                    tableLatest.put(key, currentValue);
                }
             }
            flushPending(pending, latest);
    }

    private boolean todoNeedsFlush() {
        int size = todo.size();
        return (size > 0 && (size >= FLUSH_MAX_ENTRIES || Time.since(lastFlush) > FLUSH_INTERVAL_MS));
    }

    private boolean todoCanFlush() {
        return todo.size() > 0;
    }

    private void flushPending(List<Pending> pending, Map<String,Map<List,Dbv>> latest) {
//        System.out.println("CuratorBolt3.flushPending: " + pending.size());
//        totalFlushed += pending.size();
//        String dest = changesArchive.toString();
//        System.out.println("CuratorBolt.flushPending to: " + dest + ", items: " + pending.size() + ", total: " + totalFlushed);
        List<Change> archiveBatch = new ArrayList<Change>();
        Map<String,List<List>> keysByTable = new HashMap<String,List<List>>();
        Map<String,List<Dbv>> docsByTable = new HashMap<String,List<Dbv>>();

        for (Pending p : pending) {
            archiveBatch.add(p.archiveEntry);
            String table = p.table;

            List<List> keys = keysByTable.get(table);
            if (keys == null) {
                keys = new ArrayList<List>();
                keysByTable.put(table, keys);
            }
            keys.add(p.key);

            List<Dbv> docs = docsByTable.get(table);
            if (docs == null) {
                docs = new ArrayList<Dbv>();
                docsByTable.put(table, docs);
            }
            docs.add(p.doc);
        }

        // *** write each item to the archive queue
        changesArchive.putBatch(archiveBatch);

        // *** write each item to the state store
//        for (String table : keysByTable.keySet()) {
//            List<List> keys  = keysByTable.get(table);
//            List<Dbv> docs = docsByTable.get(table);
//            state.putBatch(table, keys, docs);
//        }

        for (String table : latest.keySet()) {
            List<List> ids = new ArrayList<List>();
            List<Dbv> docs = new ArrayList<Dbv>();
            Map<List, Dbv> docMap = latest.get(table);
            for (Dbv dbv : docMap.values()) {
                ids.add(dbv.getId());
                docs.add(dbv);
            }
            state.putBatch(table, ids, docs);
        }

        for (Pending p : pending) {
            collector.ack(p.tuple);
        }

        lastFlush = Time.now();
    }

    @Override
    public void cleanup() {
        this.changesArchive.cleanup();
        this.state.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private class Pending {
        final Tuple tuple;
        String table;
        List key;
        Dbv doc;
        Change archiveEntry;

        private Pending(Tuple tuple, String table, List key, Dbv doc, Change archiveEntry) {
            this.tuple = tuple;
            this.table = table;
            this.key = key;
            this.doc = doc;
            this.archiveEntry = archiveEntry;
        }
    }

}
