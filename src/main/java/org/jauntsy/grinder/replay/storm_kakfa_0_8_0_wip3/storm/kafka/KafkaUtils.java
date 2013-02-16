package org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.utils.Utils;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

public class KafkaUtils {
    
    
     public static BatchMeta emitPartitionBatchNew(KafkaConfig config, HostPortPartition partition, SimpleConsumer consumer, TransactionAttempt attempt, BatchOutputCollector collector, BatchMeta lastMeta) {
         long offset = 0;
         if(lastMeta!=null) {
             offset = lastMeta.nextOffset;
         }
         ByteBufferMessageSet msgs;
         try {
            msgs = consumer.fetch(new FetchRequest(config.topic, partition.partition, offset, config.fetchSizeBytes));
         } catch(Exception e) {
             if(e instanceof ConnectException) {
                 throw new FailedFetchException(e);
             } else {
                 throw new RuntimeException(e);
             }
         }
         long endoffset = offset;
         for(MessageAndOffset msg: msgs) {
             long nextOffset = msg.offset();
             emit(config, partition, endoffset, nextOffset, attempt, collector, msg.message());
             endoffset = nextOffset;
         }
         BatchMeta newMeta = new BatchMeta();
         newMeta.offset = offset;
         newMeta.nextOffset = endoffset;
         return newMeta;
     }
     
     public static void emit(KafkaConfig config, HostPortPartition partition, long offset, long nextOffset, TransactionAttempt attempt, BatchOutputCollector collector, Message msg) {
         List<Object> values = config.kafkaScheme.deserialize(
                 config.topic,
                 partition.hostPort.host,
                 partition.hostPort.port,
                 partition.partition,
                 offset,
                 nextOffset,
                 Utils.toByteArray(msg.payload())
         );
         List<Object> toEmit = new ArrayList<Object>();
         toEmit.add(attempt);
         toEmit.addAll(values);
         collector.emit(toEmit);           
     }
}
