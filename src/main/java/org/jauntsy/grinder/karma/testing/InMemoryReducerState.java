package org.jauntsy.grinder.karma.testing;

import backtype.storm.task.TopologyContext;
import org.jauntsy.grinder.karma.mapred.ReducerState;

import java.util.*;

/**
 * User: ebishop
 * Date: 1/17/13
 * Time: 1:12 PM
 */
public class InMemoryReducerState extends ReducerState {

    private final boolean debug;

    private transient Map<String,Map<String,String>> keyToColumns;

    public InMemoryReducerState() {
        this(false);
    }

    public InMemoryReducerState(boolean debug) {
        this.debug = debug;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, String viewName) {
        this.keyToColumns = new HashMap<String, Map<String, String>>();
    }

    @Override
    public Map<String, Map<String, String>> getBatch(Map<String, List<String>> keysToColumns) {
        if (debug) {
//            System.out.println("InMemoryReducerState.getBatch keysToColumns: " + keysToColumns);
        }
        Map<String, Map<String,String>> ret = new HashMap<String, Map<String, String>>();
        for (String key : keysToColumns.keySet()) {
            ret.put(key, getOne(key, keysToColumns.get(key)));
        }
        return ret;
    }

    public Map<String, String> getOne(String keyAsString, List<String> columns) {
        if (debug) {
//            System.out.println("InMemoryReducerState.getOne keyAsString: " + keyAsString + ", columns: " + columns);
        }
        Map<String, String> ret = new LinkedHashMap<String, String>();
        Map<String, String> columnToJson = keyToColumns.get(keyAsString);
        for (String column : columns) {
            ret.put(column, columnToJson == null ? null : columnToJson.get(column));
        }
        return ret;
    }

    public void putAll(String key, Map<String, String> nodes) {
        if (debug) {
            System.out.println("InMemoryReducerState.putAll key: " + key + ", nodes: " + nodes);
        }
        for (Map.Entry<String,String> e : nodes.entrySet()) {
            Map<String, String> columnsToJson = keyToColumns.get(key);
            if (columnsToJson == null) {
                columnsToJson = new HashMap<String,String>();
                keyToColumns.put(key, columnsToJson);
            }
            columnsToJson.put(e.getKey(), e.getValue());
        }
    }

}
