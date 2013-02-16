package org.jauntsy.grinder.replay.contrib.storage.mongo;

/**
 * User: ebishop
 * Date: 12/27/12
 * Time: 7:27 PM
 */
public abstract class MongoDbProperties<T extends MongoDbProperties> extends MongoProperties<T> {

    public static final String MONGODB_DB = "mongodb.db";

    public T db(String db) {
        return append(MONGODB_DB, db);
    }

    public String getDb() {
        return getString(MONGODB_DB);
    }

    public static class Builder extends MongoDbProperties<Builder> {

    }

}
