package org.jauntsy.grinder.karma.mapred.nextgen;

/**
 * User: ebishop
 * Date: 2/11/13
 * Time: 10:49 AM
 */
public interface KvOutputFieldsDeclarer {

    public void declare(KvFields fields);

    public void declare(boolean direct, KvFields fields);

    public void declareStream(String streamId, KvFields fields);

    public void declareStream(String streamId, boolean direct, KvFields fields);

}
