package org.jauntsy.grinder.karma.mapred;

import backtype.storm.task.TopologyContext;

import java.util.*;

/**
 * User: ebishop
 * Date: 1/19/13
 * Time: 9:04 AM
 */
@Deprecated // *** Does not help, need to hold partial trees, not full depth ones.... this really only caches the last leaf
public class CachedReducerState extends ReducerState {

    private final ReducerState impl;
    private final LinkedHashMap<String, String> cache;

    public CachedReducerState(final ReducerState state, final int numEntries) {
        this.impl = state;
        this.cache = new LinkedHashMap<String,String>(numEntries + 1, 1.1f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> stringStringEntry) {
                return this.size() > numEntries;
            }
        };
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, String viewName) {
        throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
    }

    @Override
    public Map<String, Map<String,String>> getBatch(Map<String,List<String>> keysToColumns) {
        if (true) {
            return impl.getBatch(keysToColumns);
        }
        Map<String,Map<String,String>> batchRet = new HashMap<String, Map<String, String>>();
        Map<String,List<String>> batchToFetch = new HashMap<String, List<String>>();
        for (String keyAsString : keysToColumns.keySet()) {
            Map<String,String> ret = new HashMap<String,String>();
            List<String> columnNames = keysToColumns.get(keyAsString);
            List<String> toFetch = new ArrayList<String>(columnNames.size());
            for (String column : columnNames) {
                String cacheKey = column + ":" + keyAsString;
                String value = cache.get(cacheKey);
                if (value != null || cache.containsKey(cacheKey)) {
                    ret.put(column, value);
                } else {
                    toFetch.add(column);
                }
            }
            if (toFetch.size() > 0) {
                batchToFetch.put(keyAsString, toFetch);
            }
        }
        if (batchToFetch.size() > 0) {
            Map<String, Map<String, String>> fetched = impl.getBatch(batchToFetch);
            for (String keyAsString : fetched.keySet()) {
                Map<String, String> fetchedRow = fetched.get(keyAsString);
                Map<String,String> existingRow = batchRet.get(keyAsString);
                if (existingRow == null)
                    batchRet.put(keyAsString, fetchedRow);
                else
                    existingRow.putAll(fetchedRow);
            }
        }
        return batchRet;
    }

    @Override
    public void putAll(String keyAsString, Map<String, String> columns) {
        impl.putAll(keyAsString, columns);
        for (Map.Entry<String,String> kv : columns.entrySet()) {
            String column = kv.getKey();
            String value = kv.getValue();
            String cacheKey = keyAsString + ":" + column;
            cache.put(cacheKey, value);
        }
    }

}
