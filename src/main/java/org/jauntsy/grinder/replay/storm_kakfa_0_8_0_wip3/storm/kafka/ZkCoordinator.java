package org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jauntsy.grinder.replay.storm_kakfa_0_8_0_wip3.storm.kafka.KafkaConfig.ZkHosts;
import org.apache.zookeeper.KeeperException;

public class ZkCoordinator implements PartitionCoordinator {
    public static Logger LOG = Logger.getLogger(ZkCoordinator.class);
    
    SpoutConfig _spoutConfig;
    int _taskIndex;
    int _totalTasks;
    String _topologyInstanceId;
    CuratorFramework _curator;
    Map<GlobalPartitionId, PartitionManager> _managers = new HashMap();
    List<PartitionManager> _cachedList;
    Long _lastRefreshTime = null;
    int _refreshFreqMs;
    ZkHosts _brokerConf;
    DynamicPartitionConnections _connections;
    ZkState _state;
    Map _stormConf;

    public ZkCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig spoutConfig, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId) {
        _spoutConfig = spoutConfig;
        _connections = connections;
        _taskIndex = taskIndex;
        _totalTasks = totalTasks;
        _topologyInstanceId = topologyInstanceId;
        _stormConf = stormConf;
	_state = state;

        _brokerConf = (ZkHosts) _spoutConfig.hosts;
        _refreshFreqMs = _brokerConf.refreshFreqSecs * 1000;
        try {
            _curator = CuratorFrameworkFactory.newClient(
                    _brokerConf.brokerZkStr,
                    Utils.getInt(stormConf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                    15000,
                    new RetryNTimes(Utils.getInt(stormConf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
                    Utils.getInt(stormConf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
            _curator.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        if(_lastRefreshTime==null || _managers.size() == 0 || (System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
            refresh();
            _lastRefreshTime = System.currentTimeMillis();
        }
        return _cachedList;
    }

    void refresh() {
        try {
            LOG.info("Refreshing partition manager connections");
            String topicBrokersPath = _brokerConf.brokerZkPath + "/topics/" + _spoutConfig.topic;
            String brokerInfoPath = _brokerConf.brokerZkPath + "/ids";

            Set<GlobalPartitionId> mine = new HashSet();
            try {
                List<String> children = _curator.getChildren().forPath(topicBrokersPath);
                for(String c: children) {
                    try {
                        byte[] numPartitionsData = _curator.getData().forPath(topicBrokersPath + "/" + c);
                        byte[] hostPortData = _curator.getData().forPath(brokerInfoPath + "/" + c);

                        HostPort hp = getBrokerHost(hostPortData);
                        int numPartitions = getNumPartitions(numPartitionsData);

                        for(int i=0; i<numPartitions; i++) {
                            GlobalPartitionId id = new GlobalPartitionId(hp, i);
                            if(myOwnership(id)) {
                                mine.add(id);
                            }
                        }
                    } catch(org.apache.zookeeper.KeeperException.NoNodeException e) {

                    }
                }
            } catch(KeeperException.NoNodeException e) {
                e.printStackTrace();
            }
            Set<GlobalPartitionId> curr = _managers.keySet();
            Set<GlobalPartitionId> newPartitions = new HashSet<GlobalPartitionId>(mine);
            newPartitions.removeAll(curr);

            Set<GlobalPartitionId> deletedPartitions = new HashSet<GlobalPartitionId>(curr);
            deletedPartitions.removeAll(mine);

            LOG.info("Deleted partition managers: " + deletedPartitions.toString());

            for(GlobalPartitionId id: deletedPartitions) {
                PartitionManager man = _managers.remove(id);
                man.close();
            }
            LOG.info("New partition managers: " + newPartitions.toString());

            for(GlobalPartitionId id: newPartitions) {
                PartitionManager man = new PartitionManager(_connections, _topologyInstanceId, _state, _stormConf, _spoutConfig, id);
                _managers.put(id, man);
            }

        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        _cachedList = new ArrayList<PartitionManager>(_managers.values());
        LOG.info("Finished refreshing");
    }

    @Override
    public PartitionManager getManager(GlobalPartitionId id) {
        return _managers.get(id);
    }

    private boolean myOwnership(GlobalPartitionId id) {
        int val = Math.abs(id.host.hashCode() + 23 * id.partition);
        return val % _totalTasks == _taskIndex;
    }



    private static HostPort getBrokerHost(byte[] contents) {
        try {
            String[] hostString = new String(contents, "UTF-8").split(":");
            String host = hostString[hostString.length - 2];
            int port = Integer.parseInt(hostString[hostString.length - 1]);
            return new HostPort(host, port);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }  
    
    private static int getNumPartitions(byte[] contents) {
        try {
            return Integer.parseInt(new String(contents, "UTF-8"));            
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }  
}