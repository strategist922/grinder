package org.jauntsy.grinder.karma.contrib.hbase;

import backtype.storm.task.TopologyContext;
import org.jauntsy.grinder.karma.mapred.ReducerState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * User: ebishop
 * Date: 1/17/13
 * Time: 1:30 PM
 */
public class HbaseReducerState extends ReducerState {

    private final boolean DEBUG = false;
    private final int NUM_SPLITS = 4;
             // 16 x 16 / 2 == 1350 op/s at 50k !!!! actual == 370 op/s
             // 16 x 16 / 4 == 280 op/s at 50k inserts, 2nd run: 280 (actual == 50 op/s)
             // 16 x 16 / 8 == 256 op/s at 14k inserts

    private final Map<String,String> overrides;

    private transient Configuration conf;
    private transient HTable table;

    private transient long lastReport = 0;
    private transient long numReads = 0;
    private transient long totalTimeReading = 0;
    private transient String viewName;

    public HbaseReducerState() {
        this(new HashMap<String,String>());
    }

    public HbaseReducerState(Map overrides) {
        this.overrides = overrides;
    }

    public HbaseReducerState hbaseMaster(String hbaseMaster) {
        return set("hbase.master", hbaseMaster);
    }

    public HbaseReducerState hbaseZookeeperQuorum(String value) {
        return set("hbase.zookeeper.quorum", value);
    }

    private HbaseReducerState set(String key, String value) {
        overrides.put(key, value);
        return this;
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, String viewName) {
        try {
            System.out.println("HbaseReducerState.prepare: " + viewName);
            this.conf = HBaseConfiguration.create();
            for (String key : overrides.keySet()) {
                conf.set(key, overrides.get(key));
            }
            this.table = new HTable(this.conf, viewName);
            System.out.println("HTable built => autoFlush: " + table.isAutoFlush());
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
        this.viewName = viewName;
    }

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final byte[] FAMILY = Bytes.toBytes("increments");

    public Map<String,Map<String,String>> getBatch(Map<String,List<String>> keyToColumns) {
        try {
            long start = System.currentTimeMillis();
            List<String> keys = new ArrayList<String>();
            List<Get> gets = new ArrayList<Get>();
            for (String keyAsString : keyToColumns.keySet()) {
                keys.add(keyAsString);
                gets.add(buildGet(keyAsString, keyToColumns.get(keyAsString)));
            }
            Result[] results = table.get(gets);
            Map<String,Map<String,String>> ret = new HashMap<String,Map<String,String>>();
            for (int i = 0; i < results.length; i++) {
                String keyAsString = keys.get(i);
                List<String> columns = keyToColumns.get(keyAsString);
                Result result = results[i];
                Map<String,String> row = new HashMap<String,String>();
                for (String column : columns) {
                    byte[] value = result.getValue(FAMILY, Bytes.toBytes(column));
                    row.put(column, value == null ? null : Bytes.toString(value));
                }
                ret.put(keyAsString, row);
            }
            long now = System.currentTimeMillis();
            long dur = now - start;
            totalTimeReading += dur;
            numReads += 1;//keyToColumns.size();
            if (DEBUG && (totalTimeReading > 0 && now - lastReport > 10000)) {
                long qps = 1000L * numReads / totalTimeReading;
                System.out.println(viewName + " hbase qps: " + qps + " (" + (qps == 0 ? -1 : (1000 / qps)) + " ms)");
                lastReport = now;
            }
            return ret;
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Map<String,String> getAllOld(String keyAsString, List<String> columnNames) {
        if (DEBUG) System.out.println("HbaseReducerState.getAll key: " +keyAsString + ", cols: " + columnNames);
        try {
            long start = System.currentTimeMillis();
            Get get = buildGet(keyAsString, columnNames);
            Result result = table.get(get);
            Map<String,String> ret = new LinkedHashMap<String, String>();
            for (String columnName : columnNames) {
                byte[] value = result.getValue(FAMILY, Bytes.toBytes(columnName));
                ret.put(columnName, value == null ? null : Bytes.toString(value));
            }
            long now = System.currentTimeMillis();
            long dur = now - start;
            totalTimeReading += dur;
            numReads++;
            if (DEBUG && (totalTimeReading > 0 && now - lastReport > 10000)) {
                long qps = 1000L * numReads / totalTimeReading;
                System.out.println(viewName + " hbase qps: " + qps);
                lastReport = now;
            }
            return ret;
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private Get buildGet(String keyAsString, List<String> columnNames) {
        Get get = new Get(Bytes.toBytes(keyAsString));
        for (String columnName : columnNames) {
            get.addColumn(FAMILY, Bytes.toBytes(columnName));
        }
        return get;
    }

    @Override
    public void putAll(String keyAsString, Map<String, String> columns) {
        if (DEBUG) {
            System.out.println("HbaseReducerState.putAll key: " + keyAsString);// + ", columns: " + columns);
            for (Map.Entry<String,String> put : columns.entrySet())
                System.out.println("        col: " + put.getKey() + ", val: " + put.getValue());
        }
        try {
            Put put = new Put(Bytes.toBytes(keyAsString));
            for (String columnName : columns.keySet()) {
                String value = columns.get(columnName);
                put.add(FAMILY, Bytes.toBytes(columnName), value == null ? null : Bytes.toBytes(value));
            }
            table.put(put);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
