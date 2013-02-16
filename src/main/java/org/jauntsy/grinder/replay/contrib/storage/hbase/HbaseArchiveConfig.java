package org.jauntsy.grinder.replay.contrib.storage.hbase;

import org.jauntsy.grinder.replay.api.ArchiveClient;
import org.jauntsy.grinder.replay.api.ArchiveConfig;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
* User: ebishop
* Date: 12/29/12
* Time: 11:44 AM
*/
public abstract class HbaseArchiveConfig<T extends HbaseArchiveConfig> implements ArchiveConfig {

    private Configuration configuration;
    private String tableName;

    protected HbaseArchiveConfig() {

    }

    protected HbaseArchiveConfig(Configuration hbaseConfiguration, String tableName) {
        this.configuration = hbaseConfiguration;
        this.tableName = tableName;
    }

    @Override
    public ArchiveClient buildArchive(Map conf) {
        try {
            return new HbaseArchive(configuration, tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static HbaseArchiveConfig build(Configuration hbaseConfiguration, String tableName) {
        return new HbaseArchiveConfig(hbaseConfiguration, tableName) {};
    }

}
