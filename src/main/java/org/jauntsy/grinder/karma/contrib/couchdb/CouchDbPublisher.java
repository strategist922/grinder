package org.jauntsy.grinder.karma.contrib.couchdb;

import backtype.storm.tuple.Fields;
import org.jauntsy.grinder.karma.contrib.StateBolt;
import org.jauntsy.grinder.karma.publish.Publisher;

/**
 * User: ebishop
 * Date: 2/14/13
 * Time: 3:40 PM
 */
public class CouchDbPublisher implements Publisher {

    private final String couchDbBaseUrl;
    private final String login;
    private final String password;
    private final String dbName;

    public CouchDbPublisher(String couchDbBaseUrl, String login, String password, String dbName) {
        this.couchDbBaseUrl = couchDbBaseUrl;
        this.login = login;
        this.password = password;
        this.dbName = dbName;
    }

    @Override
    public StateBolt newBolt(String table, Fields idFields) {
        return new CouchDbBolt(couchDbBaseUrl, login, password, dbName, table, idFields);
    }
}
