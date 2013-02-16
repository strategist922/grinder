package com.exacttarget.kafka.config;

/**
 * User: ebishop
 * Date: 12/21/12
 * Time: 4:31 PM
 */
public abstract class AsyncProducerProperties<K,M,T extends AsyncProducerProperties<K,M,?>> extends ProducerProperties<K,M,T> {

    public AsyncProducerProperties() {
        put("producer.type", "async");
    }

    public T queueTime(long ms) {
        return append(QUEUE_TIME, ms);
    }

    public T queueSize(int items) {
        return append(QUEUE_SIZE, items);
    }

    public T batchSize(int items) {
        return append(BATCH_SIZE, items);
    }

}
