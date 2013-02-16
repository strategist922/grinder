package org.jauntsy.grinder.replay.api;

import kafka.api.FetchRequest;
import kafka.message.MessageAndOffset;

import java.io.Closeable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 12/16/12
 * Time: 12:38 PM
 */
public interface ArchiveClient extends Serializable {

    Partition getPartition(String topic, String host, int port, int partition);

//    ReplayArchive open(Map conf);

    void close();

    BrokerArchive getBroker(String host, int port);

    public interface Partition extends Serializable {

        public List<MessageAndOffset> fetch(long offset, int maxSize);

        public long[] getOffsetsBefore(long time, int maxNumOffsets);

        public void send(long offset, MessageAndOffset messageAndOffset);

        public void send(Map<Long, MessageAndOffset> messagesAndOffsets);

    }

    public interface BrokerArchive extends Closeable {

        List<MessageAndOffset> fetch(FetchRequest request);

        void send(String topic, int partition, long offset, MessageAndOffset messageAndOffset);

        void open();

        void close();

    }

}
