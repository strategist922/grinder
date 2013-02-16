package com.exacttarget.spike.experiments;

import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: ebishop
 * Date: 12/19/12
 * Time: 1:44 PM
 *
 * Tests what happens when Kafka expires an offset.
 * Segments are deleted after a certain amount of time.
 * This makes old offsets invalid, but what errors are thrown if any?
 *
 * Result: When offsets are expired and then queried, a NoSuchOffset exception is thrown.
 * No distinction is made between too old and too new.
 *
 */
public class ExpiredOffsets {
    public static void main(String[] args) throws IOException {
        new ExpiredOffsets().run();
    }

    private void run() throws IOException {
        Logger.getRootLogger().setLevel(Level.WARN);
        final LocalKafkaBroker broker = new LocalKafkaBroker.Builder()
                .brokerid(0)
                .enableZookeeper(false)
                .logFileSize(1000000) // 1 MB
                .logRetentionSize(1000000) // 1 MB
                .logRetentionHours(1)
                .buildLocalKafkaBroker();

        final AtomicBoolean exit = new AtomicBoolean(false);

        new Thread(new Runnable() {
            @Override
            public void run() {
                Producer<Long,Message> producer = broker.buildSyncProducer();
                List<ProducerData<Long,Message>> tenMessages = new ArrayList() {{
                    for (int i = 0; i < 10; i++) {
                        String msg = "Hello, world! (" + new Date() + ")";
                        add(new ProducerData<Long,Message>("test", new Message(msg.getBytes())));
                    }
                }};
                int wrote = 0;
                while (!exit.get()) {
                    producer.send(tenMessages);
                    wrote += tenMessages.size();
                    Utils.sleep(1);
                    if ((wrote % 10000) == 0) {
                        System.out.println("wrote = " + wrote);
                    }
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    SimpleConsumer consumer = broker.buildSimpleConsumer();
                    while (!exit.get()) {
                        Message m = null;
                        for (MessageAndOffset mno : consumer.fetch(new FetchRequest("test", 0, 0, 10000))) {
                            if (m == null) m = mno.message();
                        }
                        if (m != null) {
                            System.out.println("Fetched: " + new String(Utils.toByteArray(m.payload())) + " at " + new Date());
                        } else {
                            System.out.println("null at " + new Date());
                        }
                        Utils.sleep(5000);
                    }
                } catch(Exception ex) {
                    ex.printStackTrace();
                    exit.set(true);
                }
            }
        }).start();

        while (!exit.get()) {
            Utils.sleep(10000);
        }

        System.out.println("Exiting");

        broker.shutdown();
    }
}
