package org.jauntsy.grinder.replay.contrib.storage.mongo;

import org.jauntsy.nice.PropertiesBuilder;
import com.mongodb.Mongo;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

/**
 * User: ebishop
 * Date: 12/20/12
 * Time: 11:41 AM
 */
public abstract class MongoProperties<T extends MongoProperties> extends PropertiesBuilder<T> {

    public static final String MONGODB_HOST = "mongodb.host";
    public static final String MONGODB_PORT = "mongodb.port";

    public T host(String host) {
        return append(MONGODB_HOST, host);
    }

    public T port(int port) {
        return append(MONGODB_PORT, port);
    }

    public Mongo buildMongo(Map config) throws UnknownHostException {
        Properties p = new Properties();
        p.putAll(this);
        if (config != null) {
            for (Object key : config.keySet()) {
                if (key instanceof String && ((String)key).startsWith("mongodb.")) {
                    p.put(key, String.valueOf(config.get(key)));
                }
            }
        }
        if (
                p.containsKey(MONGODB_HOST) &&
                p.containsKey(MONGODB_PORT)
        ) {
            String host = p.getProperty(MONGODB_HOST);
            Integer port = getInteger(MONGODB_PORT);
            return new Mongo(host, port);
        } else if (p.containsKey(MONGODB_HOST)) {
            String host = p.getProperty(MONGODB_HOST);
            return new Mongo(host);
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static class Builder extends MongoProperties<Builder> {}

}
