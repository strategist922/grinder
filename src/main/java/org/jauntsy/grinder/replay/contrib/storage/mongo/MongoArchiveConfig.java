package org.jauntsy.grinder.replay.contrib.storage.mongo;

/**
* User: ebishop
* Date: 12/26/12
* Time: 10:48 AM
*/
public abstract class MongoArchiveConfig<T extends MongoArchiveConfig> extends MongoArchiveClientProperties<T> {

    public MongoArchiveConfig() {
        super();
    }

    public MongoArchiveConfig(String host) {
        super(host);
    }

    public MongoArchiveConfig(String host, int port) {
        super(host, port);
    }

    public static class Builder extends MongoArchiveConfig<Builder> {

    }

}
