package org.jauntsy.grinder.replay.testing;

import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.replay.api.SimpleReplayConsumer;
import org.jauntsy.grinder.replay.api.ArchiveClient;
import org.jauntsy.grinder.replay.api.ArchiveConfig;
import org.jauntsy.grinder.replay.base.BaseArchiveClient;
import com.exacttarget.spike.util.Databus;
import org.jauntsy.nice.PropertiesBuilder;
import junit.framework.TestCase;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.io.IOException;
import java.util.*;

/**
 * User: ebishop
 * Date: 12/16/12
 * Time: 12:54 PM
 */
public class InMemoryStore extends BaseArchiveClient {

    private Databus.Reference<HashMap<String, ArchiveClient.Partition>> partitionsRef;

    private transient HashMap<String, ArchiveClient.Partition> partitions;

    public InMemoryStore() {
        this(UUID.randomUUID().toString());
    }

    public InMemoryStore(String uuid) {
        partitionsRef = new Databus.Reference<HashMap<String, ArchiveClient.Partition>>(uuid);
        if (partitionsRef.get() == null) {
            partitionsRef.putIfEmpty(new HashMap<String, ArchiveClient.Partition>());
        }
        partitions = partitionsRef.get();
    }

    private String buildKey(String topic, String host, int port, int partition) {
        return host + ":" + port + ":" + partition + "/" + topic;
    }

    @Override
    public ArchiveClient.Partition getPartition(String topic, String host, int port, int partition) {
        String key = buildKey(topic, host, port, partition);
        ArchiveClient.Partition ret = partitionsRef.get().get(key);
        if (ret == null) {
            ret = new Partition(host, port, topic, partition);
            partitions.put(key, ret);
        }
        return ret;
    }

//    @Override
//    public InMemoryStore open(Map conf) {
//        this.partitions = partitionsRef.get();
//        return this;
//    }
//
    @Override
    public void close() {
        partitionsRef.release();
    }

    public static class Partition implements ArchiveClient.Partition {

        private static Map<String,Map<Long,MessageAndOffset>> singleton = new TreeMap<String, Map<Long, MessageAndOffset>>();

        private final String host;
        private final int port;
        private final String topic;
        private final int partition;

        private transient Map<Long,MessageAndOffset> messagesAndOffsets;

        public Partition(String host, int port, String topic, int partition) {
            this.host = host;
            this.port = port;
            this.topic = topic;
            this.partition = partition;
            open();
        }

        @Override
        public List<MessageAndOffset> fetch(long offset, int maxSize) {
            int sizeInBytes = 0;
            List ret = new ArrayList<MessageAndOffset>();
            Long next = offset;
            while (next != null && sizeInBytes < maxSize) {
                MessageAndOffset mno = messagesAndOffsets.get(next);
                next = null;
                if (mno != null) {
                    Message message = mno.message();
                    int payloadSize = message.payloadSize();
                    sizeInBytes += payloadSize;
                    if (sizeInBytes < maxSize) {
                        ret.add(mno);
                        next = mno.offset();
                    }
                }
            }
            return ret;
        }

        @Override
        public long[] getOffsetsBefore(long time, int maxNumOffsets) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void send(long offset, MessageAndOffset messageAndOffset) {
            messagesAndOffsets.put(offset, new MessageAndOffset(new Message(Utils.toByteArray(messageAndOffset.message().payload())), messageAndOffset.offset()));
        }

        @Override
        public void send(Map<Long, MessageAndOffset> messagesAndOffsets) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        private void open() {
            String key = host + ":" + port + ":" + partition + "/" + topic;
            this.messagesAndOffsets = singleton.get(key);
            if (this.messagesAndOffsets == null) {
                this.messagesAndOffsets = new TreeMap<Long, MessageAndOffset>();
                singleton.put(key, messagesAndOffsets);
            }
        }

    }

    public static abstract class ConfigBase<T extends ConfigBase> extends PropertiesBuilder<T> implements ArchiveConfig {

        public ConfigBase() {
            append("in.memory.archive.uuid", UUID.randomUUID().toString());
        }

        @Override
        public InMemoryStore buildArchive(Map conf) {
            return new InMemoryStore(getString("in.memory.archive.uuid"));
        }
    }

    public static class Config extends ConfigBase<Config> {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        LocalKafkaBroker kafka = new LocalKafkaBroker();

        ArchiveClient archivist = new InMemoryStore();

        BrokerArchive brokerArchive = archivist.getBroker(kafka.getHostname(), kafka.getPort());

        long offset = 0;
        for (int i = 0; i < 10; i++) {
            Message msg = new Message(("message" + i).getBytes("UTF8"));
            long nextOffset = offset + msg.payloadSize();
            brokerArchive.send("test", 0, offset, new MessageAndOffset(msg, nextOffset));
            offset = nextOffset;
        }

        offset = 0;
        while(true) {
            boolean found = false;
            for (MessageAndOffset mno : brokerArchive.fetch(new FetchRequest("test", 0, offset, 30))) {
                Message message = mno.message();
                byte[] bytes = new byte[message.payload().remaining()];
                message.payload().get(bytes);
                System.out.println("zzz: " + new String(bytes, "UTF8"));
                offset = mno.offset();
                found = true;
            }
            if (!found)
                break;
        }


        Producer<Long,Message> producer = kafka.buildSyncProducer();

        for (int i = 0; i < 10; i++) {
            String msg = "Message " + i;
            System.out.println("Sending: " + msg);
            producer.send(new ProducerData<Long,Message>("test", new Message(msg.getBytes())));
        }

        SimpleConsumer consumer = kafka.buildSimpleConsumer();
        Thread.currentThread().sleep(1000);
        offset = 0;
        for (MessageAndOffset mno : consumer.fetch(new FetchRequest("test", 0, offset, 1024))) {
            String s = new String(Utils.toByteArray(mno.message().payload()));
            System.out.println("consumer got " + offset + ": " + s);
            brokerArchive.send("test", 0, offset, mno);
            offset = mno.offset();
        }

        System.out.println("Pulling from archive");
        offset = 0;
        for (MessageAndOffset mno : brokerArchive.fetch(new FetchRequest("test", 0, offset, 100000))) {
            Message message = mno.message();
            String s = new String(Utils.toByteArray(message.payload()));
            System.out.println("archive got " + offset + ": " + s);
            offset = mno.offset();
        }
        System.out.println("Done");

        System.out.println("Pulling from simplereplayconsumer");
        SimpleReplayConsumer replayConsumer = new SimpleReplayConsumer(consumer, archivist, true);
        offset = 0;
        for (MessageAndOffset mno : replayConsumer.fetch(new FetchRequest("test", 0, offset, 100000))) {
            Message message = mno.message();
            String s = new String(Utils.toByteArray(message.payload()));
            System.out.println("simplereplayconsumer got " + offset + ": " + s);
            offset = mno.offset();
        }
        System.out.println("Done");
    }

    public static class Test extends TestCase {
        public void testIt() {
            InMemoryStore store = new InMemoryStore();
        }
    }
}
