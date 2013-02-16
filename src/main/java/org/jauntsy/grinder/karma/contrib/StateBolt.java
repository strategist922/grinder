package org.jauntsy.grinder.karma.contrib;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.karma.mapred.nextgen.BaseRichBatchBolt;

/**
 * User: ebishop
 * Date: 2/7/13
 * Time: 2:08 PM
 */
public abstract class StateBolt extends BaseRichBatchBolt {

    protected final Fields idFields;

    protected StateBolt(Fields idFields, int maxBatchSize, int maxBatchDelay) {
        super(maxBatchSize, maxBatchDelay);
        this.idFields = idFields;
    }

    protected boolean isDelete(Tuple input) {
        int nullCount = 0;
        for (int i = idFields.size(); i < input.size(); i++) {
            if (null == input.getValue(i))
                nullCount++;
        }
        return nullCount == (input.size() - idFields.size());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
