package org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BasePartitionedTransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaConfig.StaticHosts;


public class TransactionalKafkaSpout extends BasePartitionedTransactionalSpout<BatchMeta> {
    public static final String ATTEMPT_FIELD = TransactionalKafkaSpout.class.getCanonicalName() + "/attempt";
    
    KafkaConfig _config;
    
    public TransactionalKafkaSpout(KafkaConfig config) {
        _config = config;
    }
    
    class Coordinator implements IPartitionedTransactionalSpout.Coordinator {
        @Override
        public int numPartitions() {
            return computeNumPartitions();
        }

        @Override
        public void close() {
        }
        
        @Override
        public boolean isReady() {
            //TODO: can do a more sophisticated strategy by looking at the high water marks for each partition
            return true;
        }
    }
    
    class Emitter implements IPartitionedTransactionalSpout.Emitter<BatchMeta> {
        StaticPartitionConnections _connections;
        int partitionsPerHost;
        
        public Emitter() {
            _connections = new StaticPartitionConnections(_config);
            StaticHosts hosts = (StaticHosts) _config.hosts;
            partitionsPerHost = hosts.partitionsPerHost;            
        }
        
        @Override
        public BatchMeta emitPartitionBatchNew(TransactionAttempt attempt, BatchOutputCollector collector, int partition, BatchMeta lastMeta) {
            HostPortPartition hostPortPartition = _connections.getHostPortPartition(partition);
            SimpleConsumer consumer = _connections.getConsumer(partition);

            return KafkaUtils.emitPartitionBatchNew(_config, hostPortPartition, consumer, attempt, collector, lastMeta);
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt attempt, BatchOutputCollector collector, int partition, BatchMeta meta) {
            HostPortPartition hostPortPartition = _connections.getHostPortPartition(partition);
            SimpleConsumer consumer = _connections.getConsumer(partition);

            ByteBufferMessageSet msgs = consumer.fetch(new FetchRequest(_config.topic, hostPortPartition.partition, meta.offset, _config.fetchSizeBytes));
            long offset = meta.offset;
            for(MessageAndOffset msg: msgs) {
                if(offset == meta.nextOffset) break;
                if(offset > meta.nextOffset) {
                    throw new RuntimeException("Error when re-emitting batch. overshot the end offset");
                }
                long nextOffset = msg.offset();
                KafkaUtils.emit(_config, hostPortPartition, offset, nextOffset, attempt, collector, msg.message());
                offset = nextOffset;
            }            
        }
        
        @Override
        public void close() {
            _connections.close();
        }
    }
    

    @Override
    public IPartitionedTransactionalSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator();
    }

    @Override
    public IPartitionedTransactionalSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new Emitter();
    }    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fields = new ArrayList<String>(_config.kafkaScheme.getOutputFields().toList());
        fields.add(0, ATTEMPT_FIELD);
        declarer.declare(new Fields(fields));
    }
    
    private int computeNumPartitions() {
        StaticHosts hosts = (StaticHosts) _config.hosts;
        return hosts.hosts.size() * hosts.partitionsPerHost;      
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        backtype.storm.Config conf = new backtype.storm.Config();
        conf.registerSerialization(BatchMeta.class);
        return conf;
    }
}