package com.exacttarget.kafka.config;

/**
 * User: ebishop
 * Date: 12/21/12
 * Time: 4:31 PM
 */
public abstract class SyncProducerProperties<K,M,T extends SyncProducerProperties<K,M,?>> extends ProducerProperties<K,M,T> {

    public SyncProducerProperties() {
        put("producer.type", "sync");
    }

}
