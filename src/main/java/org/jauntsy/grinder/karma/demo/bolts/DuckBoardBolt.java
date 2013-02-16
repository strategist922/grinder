package org.jauntsy.grinder.karma.demo.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import org.jauntsy.grinder.karma.contrib.StateBolt;
import org.jauntsy.grinder.panama.UniversalComparator;
import us.monoid.json.JSONObject;
import us.monoid.web.Resty;

import java.io.IOException;
import java.util.*;

import static us.monoid.web.Resty.content;

/**
 * User: ebishop
 * Date: 2/8/13
 * Time: 3:18 PM
 */
public abstract class DuckBoardBolt extends StateBolt {

    protected final String widgetId;
    protected final String url;

    protected transient Resty resty;
    protected transient OutputCollector collector;

    protected DuckBoardBolt(Fields idFields, String widgetId) {
        super(idFields, 1000, 1000);
        this.widgetId = widgetId;
        this.url = "https://push.ducksboard.com/v/" + widgetId;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        String API_KEY = "DtobInc3mmZr4QasXReIpSbegOX1EEyywaJ3kNHXn44MULz7rC";
        this.resty = new Resty();
        resty.authenticate("https://push.ducksboard.com", API_KEY, new char[] {'x'});
        this.collector = collector;
    }

    protected void post(Map<?,?> map) throws IOException {
        JSONObject someJson = new JSONObject(map);
        resty.json(url, content(someJson));
    }

    @Override
    public final void executeBatch(List<Tuple> inputs) {
        try {
            Map<List,Tuple> lastValuesMap = new TreeMap<List,Tuple>(UniversalComparator.LIST_COMPARATOR);
            for (Tuple tuple : inputs) {
                Tuple prev = lastValuesMap.put(tuple.select(idFields), tuple);
                if (prev != null) {
                    collector.ack(prev);
                }
            }
            List<Tuple> finalList = new ArrayList<Tuple>(lastValuesMap.values());
            updateSnapshot(finalList);
        } catch(Exception ex) {
            ex.printStackTrace();
            System.out.println("Ducksboard: Backoff for five seconds");
            Utils.sleep(5000);
        }
    }

    protected final void updateSnapshot(List<Tuple> tuples) {
        Iterator<Tuple> itor = tuples.iterator();
        while (itor.hasNext()) {
            Tuple tuple = itor.next();
            try {
                updateOne(tuple);
                collector.ack(tuple);
            } catch(Exception ex) {
                ex.printStackTrace();
                Utils.sleep(5000);
                collector.fail(tuple);
                while (itor.hasNext())
                    collector.fail(itor.next());
            }
        }
    }

    protected abstract void updateOne(Tuple tuple) throws IOException;

}
