package org.jauntsy.grinder.replay.contrib.storage.mongo;

import backtype.storm.utils.Utils;
import org.jauntsy.grinder.replay.base.BaseArchiveClient;
import com.mongodb.*;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.net.UnknownHostException;
import java.util.*;

/**
 * User: ebishop
 * Date: 12/19/12
 * Time: 1:24 PM
 */
public class MongoArchiveClient extends BaseArchiveClient {

    private final MongoArchiveClientProperties config;
    private final Mongo mongo;
    private final DB db;
    private final Map<String,DBCollection> topicCols;

    public MongoArchiveClient(MongoArchiveClientProperties config) throws UnknownHostException {
        this.config = config;
        this.mongo = this.config.buildMongo(config);
        this.db = mongo.getDB(config.getDb());
        this.topicCols = new HashMap<String,DBCollection>();
    }

    private DBCollection getTopicCol(String topic) {
        synchronized (topicCols) {
            DBCollection col = topicCols.get(topic);
            if (col == null) {
                col = db.getCollection(topic);
                col.ensureIndex(new BasicDBObject().append("host", 1).append("port",1).append("partition", 1).append("offset", 1));
                topicCols.put(topic, col);
            }
            return col;
        }
    }

    @Override
    public Partition getPartition(String topic, String host, int port, int partition) {
        return new MongoDbPartition(topic, host, port, partition, getTopicCol(topic));
    }

    @Override
    public void close() {
        this.mongo.close();
    }

    private class MongoDbPartition implements Partition {

        private final String topic;
        private final String host;
        private final int port;
        private final int partition;
        private final DBCollection coll;

        public MongoDbPartition(String topic, String host, int port, int partition, DBCollection coll) {
            this.topic = topic;
            this.host = host;
            this.port = port;
            this.partition = partition;
            this.coll = coll;
        }

        @Override
        public List<MessageAndOffset> fetch(long offset, int maxSize) {
            List ret = new ArrayList();
            DBObject query = new BasicDBObject().append("host", host).append("port", port).append("partition", partition).append("offset", new BasicDBObject("$gte", offset));
            long nextExpectedOffset = offset;
            int sizeInBytes = 0;
            for (DBObject dbo : coll.find(query).sort(new BasicDBObject("offset", 1))) {
                long _offset = ((Number)dbo.get("offset")).longValue();
                if (_offset == nextExpectedOffset) {
                    byte[] _bytes = (byte[])dbo.get("bytes");
                    sizeInBytes += _bytes.length;
                    if (sizeInBytes <= maxSize) {
                        long _next = ((Number)dbo.get("next")).longValue();
                        ret.add(new MessageAndOffset(new Message(_bytes), _next));
                        nextExpectedOffset = _next;
                    } else {
                        break;
                    }
                }
            }
            return ret;
        }

        public List<MessageAndOffset> fetchOne(long offset) {
            List ret = new ArrayList();
            DBObject one = coll.findOne(new BasicDBObject("_id", toMongoId(host, port, partition, offset)));
            if (one != null) {
                ret.add(new MessageAndOffset(new Message((byte[])one.get("bytes")), ((Number)one.get("next")).longValue()));
            }
            return ret;
        }

        @Override
        public long[] getOffsetsBefore(long time, int maxNumOffsets) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void send(long offset, MessageAndOffset messageAndOffset) {
            coll.save(new BasicDBObject("_id", toMongoId(host, port, partition, offset))
                    .append("host", host)
                    .append("port", port)
                    .append("partition", partition)
                    .append("offset", offset)
                    .append("bytes", Utils.toByteArray(messageAndOffset.message().payload()))
                    .append("next", messageAndOffset.offset()),
                    WriteConcern.NORMAL
            );
        }

        private String toMongoId(String host, int port, int partition, long offset) {
            return new StringBuilder(host).append(":").append(port).append(":").append(partition).append(":").append(topic).append(":").append(offset).toString();
        }

        @Override
        public void send(Map<Long, MessageAndOffset> messagesAndOffsets) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

    }

}
