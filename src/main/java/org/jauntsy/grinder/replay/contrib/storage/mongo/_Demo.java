package org.jauntsy.grinder.replay.contrib.storage.mongo;

import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.replay.api.ArchiveClient;
import org.jauntsy.grinder.replay.api.SimpleReplayConsumer;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.io.IOException;

/**
 * User: ebishop
 * Date: 12/28/12
 * Time: 2:59 PM
 */
public class _Demo {
    public static void main(String[] args) throws IOException {
        LocalKafkaBroker broker = new LocalKafkaBroker(0, 9090, 1);
        MongoArchiveClient mongoArchive = new MongoArchiveConfig.Builder().host("localhost").buildArchive(null);

        Producer<Long,Message> producer = broker.buildSyncProducer();

        for (int i = 0; i < 10; i++) {
            String msg = "Hello, " + i;
            producer.send(new ProducerData("test", new Message(msg.getBytes("UTF8"))));
        }
        Utils.sleep(4000);

        // *** fill up mongo (this is usually done by a daemon subscriber)
        if (true) {
            SimpleConsumer consumer = new SimpleConsumer("localhost", 9090, 100000, 100000);
            ArchiveClient.Partition partition = mongoArchive.getPartition("test", "localhost", 9090, 0);
            long offset = 0L;
            while (true) {
                int found = 0;
                for (MessageAndOffset mno : consumer.fetch(new FetchRequest("test", 0, offset, 10000))) {
                    byte[] bytes = Utils.toByteArray(mno.message().payload());
                    partition.send(offset, mno);
                    offset = mno.offset();
                    found++;
                }
                if (found == 0)
                    break;
            }
            consumer.close();
        }

        // *** consume the queue
        if (true) {
            SimpleReplayConsumer consumer = new SimpleReplayConsumer("localhost", 9090, 100000, 100000, mongoArchive);
            long offset = 0L;
            while (true) {
                int found = 0;
                for (MessageAndOffset mno : consumer.fetch(new FetchRequest("test", 0, offset, 10000))) {
                    System.out.println(new String(Utils.toByteArray(mno.message().payload()), "UTF8"));
                    offset = mno.offset();
                    found++;
                }
                if (found == 0)
                    break;
            }
            consumer.close();
        }

        // *** consume the queue from mongo
        if (true) {
            SimpleReplayConsumer consumer = new SimpleReplayConsumer("localhost", 9090, 100000, 100000, mongoArchive, true);
            long offset = 0L;
            while (true) {
                int found = 0;
                for (MessageAndOffset mno : consumer.fetch(new FetchRequest("test", 0, offset, 10000))) {
                    System.out.println(new String(Utils.toByteArray(mno.message().payload()), "UTF8"));
                    System.out.println("offset = " + offset);
                    offset = mno.offset();
                    System.out.println("next = " + offset);
                    found++;
                }
                if (found == 0)
                    break;
            }
            consumer.close();
        }

        producer.close();
        broker.shutdown();
    }
}
