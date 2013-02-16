package org.jauntsy.grinder.karma.contrib.cassandra;

import backtype.storm.tuple.Fields;
import org.jauntsy.grinder.karma.contrib.StateBolt;
import org.jauntsy.grinder.karma.publish.Publisher;

/**
 * User: ebishop
 * Date: 2/14/13
 * Time: 3:38 PM
 */
public class CassandraPublisher implements Publisher {

    private final String keyspace;

    public CassandraPublisher(String keyspace) {
        this.keyspace = keyspace;
    }

    @Override
    public StateBolt newBolt(String table, Fields idFields) {
        return new CassandraBolt(keyspace, table, idFields);
    }
}
