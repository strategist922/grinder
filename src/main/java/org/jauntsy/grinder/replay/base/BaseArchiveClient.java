package org.jauntsy.grinder.replay.base;

import org.jauntsy.grinder.replay.api.ArchiveClient;
import kafka.api.FetchRequest;
import kafka.message.MessageAndOffset;

import java.util.List;

/**
 * User: ebishop
 * Date: 12/20/12
 * Time: 10:29 AM
 */
public abstract class BaseArchiveClient implements ArchiveClient {

    @Override
    public BrokerArchive getBroker(String host, int port) {
        return new BrokerAdapter(host, port);
    }

    private class BrokerAdapter implements BrokerArchive {

        private final String host;
        private final int port;

        private BrokerAdapter(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public List<MessageAndOffset> fetch(FetchRequest request) {
            return getPartition(request.topic(), host, port, request.partition()).fetch(request.offset(), request.maxSize());
        }

        @Override
        public void send(String topic, int partition, long offset, MessageAndOffset messageAndOffset) {
            getPartition(topic, host, port, partition).send(offset, messageAndOffset);
        }

        @Override
        public void open() {

        }

        @Override
        public void close() {

        }
    }

}
