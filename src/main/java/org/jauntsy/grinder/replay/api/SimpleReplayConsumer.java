package org.jauntsy.grinder.replay.api;

import kafka.api.FetchRequest;
import kafka.common.ErrorMapping;
import kafka.common.OffsetOutOfRangeException;
import kafka.javaapi.MultiFetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * User: ebishop
 * Date: 12/19/12
 * Time: 2:21 PM
 */
public class SimpleReplayConsumer extends SimpleConsumer {

    public static final Logger LOG = Logger.getLogger(SimpleReplayConsumer.class);

    private ArchiveClient.BrokerArchive archive;
    private boolean forceUseArchive;

    public SimpleReplayConsumer(SimpleConsumer prototype, ArchiveClient archive) {
        this(prototype.host(), prototype.port(), prototype.soTimeout(), prototype.bufferSize(), archive, false);
    }

    public SimpleReplayConsumer(SimpleConsumer prototype, ArchiveClient archive, boolean forceUseArchive) {
        this(prototype.host(), prototype.port(), prototype.soTimeout(), prototype.bufferSize(), archive, forceUseArchive);
    }

    public SimpleReplayConsumer(String host, int port, int soTimeout, int bufferSize, ArchiveClient archive) {
        this(host, port, soTimeout, bufferSize, archive, false);
    }

    public SimpleReplayConsumer(String host, int port, int soTimeout, int bufferSize, ArchiveClient archive, boolean forceUseArchive) {
        super(host, port, soTimeout, bufferSize);
        if (archive != null) {
            this.archive = archive.getBroker(host, port);
            this.archive.open();
        }
        this.forceUseArchive = forceUseArchive;
    }

    @Override
    public ByteBufferMessageSet fetch(FetchRequest request) {
        try {
            if (forceUseArchive)
                throw new OffsetOutOfRangeException();
            return super.fetch(request);
        } catch(OffsetOutOfRangeException ex) {
            if (archive != null) {
                LOG.warn("OffsetOutOfRange: Failing over to archive.");
                List<Message> messages = new ArrayList<Message>();
                for (MessageAndOffset mno : archive.fetch(request)) {
                    messages.add(mno.message());
                }
                return new ByteBufferMessageSet(
                        new ByteBufferMessageSet(messages).getBuffer(),
                        request.offset(),
                        ErrorMapping.NoError()
                );
            } else {
                throw ex;
            }
        }
    }

    /*
    TODO: implement this, dunno what it should do yet.
     */
    @Override
    public long[] getOffsetsBefore(String topic, int partition, long time, int maxNumOffsets) {
        return super.getOffsetsBefore(topic, partition, time, maxNumOffsets);
    }

    @Override
    public MultiFetchResponse multifetch(List<FetchRequest> fetches) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        System.out.println("SimpleReplayConsumer.close");
        archive.close();
        super.close();
    }
}
