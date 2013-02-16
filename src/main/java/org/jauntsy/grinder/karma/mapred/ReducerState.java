package org.jauntsy.grinder.karma.mapred;

import backtype.storm.task.TopologyContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 1/17/13
 * Time: 1:09 PM
 */
public abstract class ReducerState implements Serializable {

    public abstract void prepare(Map stormConf, TopologyContext context, String viewName);

    public abstract Map<String,Map<String,String>> getBatch(Map<String,List<String>> keysToColumns);

    @Deprecated
    public final Map<String,String> getAll(String keyAsString, List<String> columnNames) {
        Map<String,List<String>> batch = new HashMap<String,List<String>>();
        batch.put(keyAsString, columnNames);
        Map<String, Map<String, String>> result = getBatch(batch);
        return result.get(keyAsString);
    }

    public abstract void putAll(String keyAsString, Map<String, String> columns);

}
