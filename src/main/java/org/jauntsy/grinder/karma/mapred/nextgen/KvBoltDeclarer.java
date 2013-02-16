package org.jauntsy.grinder.karma.mapred.nextgen;

import backtype.storm.tuple.Fields;

/**
 * User: ebishop
 * Date: 2/11/13
 * Time: 10:59 AM
 */
public interface KvBoltDeclarer {
    public void kvGrouping(String sourceKvComponentId, Fields keyFieldNames);
}
