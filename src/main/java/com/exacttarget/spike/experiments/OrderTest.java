package com.exacttarget.spike.experiments;

import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: ebishop
 * Date: 12/21/12
 * Time: 3:31 PM
 *
 * Exhibits and error in SimpleConsumer
 * SimpleConsumer can get messages out of order under load when it closes and reopens
 * the connection (defaults to every 30_000 messages)
 *
 * Theory: Producer creates a new connection every 30k messages. It seems that messages from the new
 * connection are mixed in arbitrary order with messages from the old. Odd.... needs more exploration.
 *
 */
public class OrderTest {

    public static void main(String[] args) throws IOException {
        final LocalKafkaBroker broker = new LocalKafkaBroker();
        final AtomicBoolean exit = new AtomicBoolean(false);

        if (false) {
            startConsumer(broker, exit);

            Utils.sleep(5000);
            startProducer(broker, exit);
        } else {
            startProducer(broker, exit);

            Utils.sleep(10000);
            startConsumer(broker, exit);
        }

        while(!exit.get()) {
            Utils.sleep(1000);
        }
    }

    private static void startProducer(final LocalKafkaBroker broker, final AtomicBoolean exit) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                long value = 0L;
                System.out.println("Starting producer");
                Producer<Long,Message> producer = broker.buildSyncProducer();
                while (!exit.get() && value < 60000) {
//                    producer.send(new ProducerData<Long, Message>("test", 0L, Arrays.asList(new Message[] { new Message(String.valueOf(value).getBytes()) })));
                    producer.send(new ProducerData<Long, Message>("test", new Message(String.valueOf(value).getBytes())));
                    value++;
                }
                System.out.println("Exiting producer thread.");
            }
        }).start();
    }

    private static void startConsumer(final LocalKafkaBroker broker, final AtomicBoolean exit) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                SimpleConsumer consumer = broker.buildSimpleConsumer();
                long offset = 0;
                long lastValue = -1;
                while (!exit.get()) {
                    for (MessageAndOffset mno : consumer.fetch(new FetchRequest("test", 0, offset, 10000))) {
                        long value = Long.parseLong(new String(Utils.toByteArray(mno.message().payload())));
                        System.out.println("value = " + value);
                        if (value != lastValue + 1) {
//                            System.out.println("value = " + value);
                            System.out.println("ERROR: lastValue was " + lastValue + ". Expected " + (lastValue + 1) + " but got " + value + ".");
                            exit.set(true);
                            break;
                        }
                        lastValue = value;
                        offset = mno.offset();
//                        if (value % 1000 == 0) {
//                            System.out.println("value = " + value);
//                        }
                    }
                    Utils.sleep(100);
                }
                System.out.println("Exiting consumer thread.");
            }
        }).start();
    }
}
