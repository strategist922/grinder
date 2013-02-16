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

/**
 * User: ebishop
 * Date: 12/20/12
 * Time: 9:48 AM
 *
 * Tests what happens when you ask for an offset from the future.
 * This could be due to client error or rollback/recovery on the server
 * side which fixes corruption by discarding the head of the queue till
 * valid entries are found.
 *
 */
public class FutureOffsets {
    public static void main(String[] args) throws IOException {
        LocalKafkaBroker broker = new LocalKafkaBroker();

        Producer<Long,Message> producer = broker.buildSyncProducer();
        producer.send(new ProducerData<Long, Message>("test", new Message("Hello, world!".getBytes())));

        SimpleConsumer consumer = broker.buildSimpleConsumer();
        for (MessageAndOffset mno : consumer.fetch(new FetchRequest("test", 0, 0, 10000)))
            System.out.println(new String(Utils.toByteArray(mno.message().payload())));

        // *** request an offset that is after the current head
        consumer.fetch(new FetchRequest("test", 0, 123456, 10000));
    }
}
