package org.jauntsy.grinder.replay.contrib.storage.mongo;

import org.jauntsy.grinder.replay.api.ArchiveConfig;

import java.util.Map;

/**
* User: ebishop
* Date: 12/26/12
* Time: 10:48 AM
*/
public abstract class MongoArchiveClientProperties<T extends MongoArchiveClientProperties> extends MongoProperties<T> implements ArchiveConfig {

    public MongoArchiveClientProperties() {
    }

    public MongoArchiveClientProperties(String host) {
        host(host);
    }

    public MongoArchiveClientProperties(String host, int port) {
        host(host);
        port(port);
    }

    public T mongoConfig(MongoProperties mongoProperties) {
        return appendAll(mongoProperties);
    }

    public T db(String db) {
        return append("mongodb.db", db);
    }

    public String getDb() {
        return getString("mongodb.db", "_replay");
    }

    @Override
    public MongoArchiveClient buildArchive(Map config) {
        try {
            return new MongoArchiveClient(this);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class Builder extends MongoArchiveClientProperties<Builder> {

    }

}
