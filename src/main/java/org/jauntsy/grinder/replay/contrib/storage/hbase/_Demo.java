package org.jauntsy.grinder.replay.contrib.storage.hbase;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * User: ebishop
 * Date: 12/28/12
 * Time: 2:59 PM
 */
public class _Demo {
    public static void main(String[] args) throws IOException {
        String tableName = "testArchive";

        Configuration hbaseConfiguration = HBaseConfiguration.create();

        HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);
        System.out.println("Connected to hbase ver " + admin.getClusterStatus().getHBaseVersion());

        deleteTableIfPresent(admin, tableName);

        LocalKafkaBroker broker = new LocalKafkaBroker(0, 9090, 1);
        ArchiveClient archive = new HbaseArchive(hbaseConfiguration, tableName);

        Producer<Long,Message> producer = broker.buildSyncProducer();

        for (int i = 0; i < 10000; i++) {
            String msg = "Hello, " + i;
            producer.send(new ProducerData("test", new Message(msg.getBytes("UTF8"))));
        }
        Utils.sleep(4000);

        // *** fill up hbase (this is usually done by a daemon subscriber)
        if (true) {
            SimpleConsumer consumer = new SimpleConsumer("localhost", 9090, 100000, 100000);
            ArchiveClient.Partition partition = archive.getPartition("test", "localhost", 9090, 0);
            long offset = 0L;
            while (true) {
                int found = 0;
                for (MessageAndOffset mno : consumer.fetch(new FetchRequest("test", 0, offset, 10000))) {
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
            SimpleReplayConsumer consumer = new SimpleReplayConsumer("localhost", 9090, 100000, 100000, archive);
            long offset = 0L;
            while (true) {
                int found = 0;
                for (MessageAndOffset mno : consumer.fetch(new FetchRequest("test", 0, offset, 10000))) {
//                    System.out.println(getString(mno.message()));
                    offset = mno.offset();
                    found++;
//                    Utils.sleep(50);
                }
                if (found == 0)
                    break;
            }
            consumer.close();
        }

        // *** consume the queue from db
        if (true) {
            SimpleReplayConsumer consumer = new SimpleReplayConsumer("localhost", 9090, 100000, 100000, archive, true);
            long offset = 0L;
            while (true) {
                int found = 0;
                for (MessageAndOffset mno : consumer.fetch(new FetchRequest("test", 0, offset, 10000))) {
//                    System.out.println(getString(mno.message()));
                    offset = mno.offset();
                    found++;
//                    Utils.sleep(50);
                }
                if (found == 0)
                    break;
            }
            consumer.close();
        }

        producer.close();
        broker.shutdown();
    }

    private static void deleteTableIfPresent(HBaseAdmin admin, String tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    private static byte[] getBytes(MessageAndOffset mno) {
        return getBytes(mno.message());
    }

    private static byte[] getBytes(Message message) {
        return Utils.toByteArray(message.payload());
    }

    private static String getString(Message message) throws UnsupportedEncodingException {
        return new String(getBytes(message), "UTF8");
    }

}
