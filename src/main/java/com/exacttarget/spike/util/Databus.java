package com.exacttarget.spike.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * User: ebishop
 * Date: 12/20/12
 * Time: 10:57 AM
 */
public class Databus {

    private static Map<String,Object> instances = new HashMap<String,Object>();
    private static Map<String,Integer> counts = new HashMap<String,Integer>();

    public static <T> Reference<T> register(T instance) {
        String uuid = UUID.randomUUID().toString();
        synchronized (instances) {
            instances.put(uuid, instance);
            counts.put(uuid, 0);
        }
        return new Reference<T>(uuid);
    }

    public static class Reference<T> implements Serializable {

        private String uuid;

        private transient T instance;

        public Reference(String uuid) {
            this.uuid = uuid;
        }

        public synchronized T get() {
            if (instance == null) {
                if (uuid == null)
                    throw new IllegalStateException();
                synchronized (instances) {
                    instance = (T)instances.get(uuid);
                    if (instance == null)
                        return null;
                    counts.put(uuid, counts.get(uuid) + 1);
                }
            }
            return instance;
        }

        public synchronized void release() {
            if (uuid != null) {
                synchronized (instances) {
                    int count = counts.get(uuid);
                    if (count == 1) {
                        instances.remove(uuid);
                        counts.remove(uuid);
                    }
                }
                uuid = null;
            }
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            release();
        }

        public void putIfEmpty(T value) {
            synchronized (instances) {
                if (!instances.containsKey(uuid)) {
                    instances.put(uuid, value);
                    counts.put(uuid, 0);
                }
            }
        }
    }
}
