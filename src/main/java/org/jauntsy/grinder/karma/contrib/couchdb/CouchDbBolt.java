package org.jauntsy.grinder.karma.contrib.couchdb;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import org.jauntsy.grinder.karma.contrib.StateBolt;
import org.jauntsy.grinder.panama.UniversalComparator;
import org.json.simple.JSONValue;
import us.monoid.json.JSONException;
import us.monoid.web.Resty;

import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.jauntsy.nice.Nice.list;
import static us.monoid.web.Resty.content;

/**
 * User: ebishop
 * Date: 2/7/13
 * Time: 1:28 PM
 */
public class CouchDbBolt extends StateBolt {

    private final String couchBaseUrl;
    private final String login;
    private final String password;
    private final String dbName;
    private final String tableName;

    private OutputCollector collector;
    private Resty resty;

    public CouchDbBolt(String couchBaseUrl, String login, String password, String dbName, String tableName, Fields idFields) {
        super(idFields, 1000, 1000);
        this.couchBaseUrl = couchBaseUrl;
        this.login = login;
        this.password = password;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.collector = collector;
        this.resty = new Resty();
        if (login != null) {
            this.resty.authenticate(couchBaseUrl, login, password.toCharArray());
        }
    }

    @Override
    public void executeBatch(List<Tuple> inputs) {
        Map<List,Tuple> m = new TreeMap<List,Tuple>(UniversalComparator.LIST_COMPARATOR);
        for (Tuple tuple : inputs) {
            Tuple prev = m.put(tuple.select(idFields), tuple);
            if (prev != null)
                collector.ack(prev);
        }
        boolean failed = false;
        for (Tuple tuple : m.values()) {
            if (failed) {
                collector.fail(tuple);
            } else {
                try {
                    executeImpl(tuple);
                    collector.ack(tuple);
                } catch(Exception ex) {
                    failed = true;
                    ex.printStackTrace();
                    Utils.sleep(5000);
                    collector.fail(tuple);
                }
            }
        }
    }

    public void executeImpl(Tuple input) {
        List _id = list(tableName);
        _id.addAll(input.select(idFields));
        String rowKey = JSONValue.toJSONString(_id);
        us.monoid.json.JSONObject jo = new us.monoid.json.JSONObject();
        try {
            jo.put("_id", rowKey);
            jo.put("type", tableName);
            for (int i = 0; i < input.size(); i++) {
                String field = input.getFields().get(i);
                Object value = input.getValue(i);
                jo.put(field, value);
            }
        } catch(JSONException ex) {
            throw new RuntimeException(ex);
        }
        try {
            String uri = isDelete(input) ?
                    couchBaseUrl + "/" + dbName + "/_design/karma_support/_update/delete/" + URLEncoder.encode(rowKey) :
                    couchBaseUrl + "/" + dbName + "/_design/karma_support/_update/replace/" + URLEncoder.encode(rowKey);
            resty.json(uri, content(jo));
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
