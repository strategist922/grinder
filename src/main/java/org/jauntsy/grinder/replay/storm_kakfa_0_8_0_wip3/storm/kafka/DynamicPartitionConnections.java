package org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jauntsy.grinder.replay.api.SimpleReplayConsumer;
import kafka.javaapi.consumer.SimpleConsumer;


public class DynamicPartitionConnections {
    static class ConnectionInfo {
        SimpleConsumer consumer;
        Set<Integer> partitions = new HashSet();
        
        public ConnectionInfo(SimpleConsumer consumer) {
            this.consumer = consumer;
        }
    }
    
    Map<HostPort, ConnectionInfo> _connections = new HashMap();
    Map _stormConf;
    SpoutConfig _config;

    public DynamicPartitionConnections(Map stormConf, SpoutConfig config) {
        _stormConf = stormConf;
        _config = config;
    }

    public SimpleConsumer register(HostPort host, int partition) {
        if(!_connections.containsKey(host)) {
            _connections.put(host, new ConnectionInfo(new SimpleReplayConsumer(host.host, host.port, _config.socketTimeoutMs, _config.bufferSizeBytes, _config.archive == null ? null : _config.archive.buildArchive(_stormConf))));
        }
        ConnectionInfo info = _connections.get(host);
        info.partitions.add(partition);
        return info.consumer;
    }

    public void unregister(HostPort port, int partition) {
        ConnectionInfo info = _connections.get(port);
        info.partitions.remove(partition);
        if(info.partitions.size()==0) {
            info.consumer.close();
        }
        _connections.remove(port);
    }
    
    public void close() {
        for(ConnectionInfo info: _connections.values()) {
            info.consumer.close();
        }
        _connections.clear();
    }
}
