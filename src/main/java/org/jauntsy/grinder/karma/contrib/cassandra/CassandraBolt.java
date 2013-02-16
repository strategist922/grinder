package org.jauntsy.grinder.karma.contrib.cassandra;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.jauntsy.grinder.karma.contrib.StateBolt;
import org.jauntsy.grinder.panama.UniversalComparator;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.json.simple.JSONValue;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.jauntsy.nice.Nice.M;

/**
 * User: ebishop
 * Date: 2/7/13
 * Time: 1:28 PM
 */
public class CassandraBolt extends StateBolt {

    public static final String HOST = "cassandra.host";
    public static final String PORT = "cassandra.port";

    private final String keyspaceName;
    private final String tableName;

    private transient AstyanaxContext<Keyspace> context;
    private transient Keyspace keyspace;
    private ColumnFamily<String, String> cf;
    private OutputCollector collector;

    public CassandraBolt(String keyspaceName, String tableName, Fields idFields) {
        super(idFields, 1000, 1000);
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        String host = (String) stormConf.get(HOST);
        if (host == null)
            host = "127.0.0.1";
        String port = (String) stormConf.get(PORT);
        if (port == null)
            port = "9160";
        this.context = new AstyanaxContext.Builder()
                .forKeyspace(keyspaceName)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.NONE)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnection")
                        .setPort(Integer.parseInt(port))
                        .setMaxConnsPerHost(1)
                        .setSeeds(host + ":" + port)
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        this.context.start();
        this.keyspace = this.context.getEntity();
        this.cf = new ColumnFamily<String, String>(
                tableName,
                StringSerializer.get(),
                StringSerializer.get()
        );
        try {
            ColumnFamilyDefinition columnFamily = keyspace.describeKeyspace().getColumnFamily(tableName);
            if (columnFamily == null) {
                try {
                    keyspace.createColumnFamily(cf, M(
                            "comparator", "UTF8Type",
                            "caching", "ALL"
                    ));
                } catch (Exception ignore) {
                    ignore.printStackTrace();
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.collector = collector;
    }

    @Override
    public void executeBatch(List<Tuple> tuples) {
//        System.out.println("CassandraBolt.execute: " + input);
        Map<List, Tuple> m = new TreeMap<List, Tuple>(UniversalComparator.LIST_COMPARATOR);
        for (Tuple tuple : tuples) {
            Tuple prev = m.put(tuple.select(idFields), tuple);
            if (prev != null) {
                collector.ack(prev);
            }
        }

        for (Tuple input : m.values()) {
            String rowKey = null;
            for (Object keyPart : input.select(idFields))
                rowKey = rowKey == null ? (String.valueOf(keyPart)) : (rowKey + "/" + String.valueOf(keyPart));
            if (rowKey == null)
                rowKey = "{}";
            MutationBatch batch = keyspace.prepareMutationBatch();
            ColumnListMutation<String> rowUpdate = batch.withRow(cf, rowKey);
            if (isDelete(input)) {
                rowUpdate.delete();
            } else {
                for (int i = 0; i < input.size(); i++) {
                    String field = input.getFields().get(i);
                    Object value = input.getValue(i);
                    rowUpdate.putColumn(field, JSONValue.toJSONString(value));
                }
            }
            try {
                batch.execute();
                collector.ack(input);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
