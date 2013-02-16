package org.jauntsy.grinder.replay.contrib.storage.mongo;

/**
 * User: ebishop
 * Date: 12/27/12
 * Time: 7:32 PM
 */
public abstract class MongoCollectionProperties<T extends MongoCollectionProperties> extends MongoDbProperties<T> {

    public static final String MONGODB_COLLECTION = "mongodb.collection";

    public T collection(String collection) {
        return append(MONGODB_COLLECTION, collection);
    }

    public String getCollection() {
        return getString(MONGODB_COLLECTION);
    }

    public static class Builder extends MongoCollectionProperties<Builder> {

    }

}
