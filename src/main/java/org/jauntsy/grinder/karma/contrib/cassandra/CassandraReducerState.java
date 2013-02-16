package org.jauntsy.grinder.karma.contrib.cassandra;

import backtype.storm.task.TopologyContext;
import org.jauntsy.grinder.karma.mapred.ReducerState;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static org.jauntsy.nice.Nice.M;

/**
 * User: ebishop
 * Date: 1/17/13
 * Time: 1:29 PM
 */
public class CassandraReducerState extends ReducerState {

    private final String keyspaceName;

    private String viewName;
    private AstyanaxContext<Keyspace> context;
    private Keyspace keyspace;
    private ColumnFamily<String, String> CF;

    public CassandraReducerState(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, String viewName) {
        this.viewName = viewName;
        this.context = new AstyanaxContext.Builder()
                .forKeyspace(keyspaceName)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.NONE)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnection")
                        .setPort(9160)
                        .setMaxConnsPerHost(1)
                        .setSeeds("127.0.0.1:9160")
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        this.context.start();
        this.keyspace = this.context.getEntity();

        this.CF = new ColumnFamily<String, String>(
                viewName,
                StringSerializer.get(),
                StringSerializer.get()
        );

        try {
            KeyspaceDefinition keyspaceDefinition = keyspace.describeKeyspace();
            ColumnFamilyDefinition columnFamily = keyspaceDefinition.getColumnFamily(viewName);
            if (columnFamily == null) {
                try {
                    System.out.println("Building column family " + CF.getName());
                    keyspace.createColumnFamily(CF, M("comparator", "UTF8Type", "caching", "ALL"));
                } catch(BadRequestException tableExists) {
                    tableExists.printStackTrace();
                }
            } else {
                System.out.println(("Using column family " + CF.getName()));
            }
        } catch(Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }

    }

    @Override
    public Map<String, Map<String, String>> getBatch(Map<String, List<String>> keysToColumns) {
        try {
            Map<String,Map<String,String>> ret = new TreeMap<String, Map<String, String>>(); // *** TODO: use hashmap
            ColumnFamilyQuery<String,String> query = keyspace.prepareQuery(CF);
            for (String skey : keysToColumns.keySet()) {
                String rowKey = skey;
                Map<String,String> row = new TreeMap<String,String>(); // *** TODO: use hashmap?
                keysToColumns.get(skey);
                OperationResult<ColumnList<String>> result = query
                        .setConsistencyLevel(ConsistencyLevel.CL_ALL)
                        .getKey(rowKey)
                        .withColumnSlice(keysToColumns.get(skey))
                        .execute();
                for (Column<String> column : result.getResult()) {
                    row.put(column.getName(), column.getStringValue());
                }
                ret.put(skey, row);
            }
            return ret;
        } catch(ConnectionException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void putAll(String keyAsString, Map<String, String> columns) {
        try {
            String rowKey = keyAsString;

            MutationBatch batch = keyspace.prepareMutationBatch();
            ColumnListMutation<String> row = batch.withRow(CF, rowKey);
            for (Map.Entry<String,String> kv : columns.entrySet()) {
                String columnName = kv.getKey();
                String columnValue = kv.getValue();
                if (columnValue != null)
                    row.putColumn(columnName, columnValue);
                else
                    row.deleteColumn(columnName);
            }
            batch.execute().getResult();
        } catch(ConnectionException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void explode(Map map) {
        println(0, "{");
        explode(1, map);
        println(0, "}");
    }

    private static void explode(int depth, Map map) {
        for (Object key : map.keySet()) {
            Object value = map.get(key);
            if (value instanceof Map) {
                println(depth, key + ": {");
                explode(depth + 1, (Map)value);
                println(depth, key + "}");
            } else if (value instanceof Number || value instanceof String || value instanceof Boolean) {
                println(depth, key + ": " + value.getClass().getSimpleName() + ": " + value);
            } else {
                throw new IllegalArgumentException(value.getClass().getName());
            }
        }
    }

    private static void println(int depth, Object o) {
        for (int i = 0; i < depth; i++)
            System.out.print("  ");
        System.out.println(o);
    }

    public static void main(String[] args) throws ConnectionException, ExecutionException, InterruptedException {
        CassandraReducerState state = new CassandraReducerState("tmptmp");
        state.prepare(null, null, "accountBalance_red");

        System.out.println("Getting one column: 3:470");
        ColumnList<String> oneResult = state.keyspace.prepareQuery(state.CF)
                .getKey("accountBalance_red:0:0:[\"p\"]")
                .withColumnSlice("3:470")
                .execute()
                .getResult();
        for (Column<String> c : oneResult) {
            System.out.println(c.getName() + ": " + c.getStringValue());
        }

        System.out.println("Getting multiple columns: 0:0, 1:1, 2:29 and 3:470");
        ColumnList<String> multiResult = state.keyspace.prepareQuery(state.CF)
                .getKey("accountBalance_red:0:0:[\"p\"]")
                .autoPaginate(true)
                .withColumnSlice(
                        "0:0",
                        "1:1",
                        "2:29",
                        "3:470"
                        //,"3:471"
                )
                .execute()
                .getResult();
        System.out.println("multiResult.getColumnNames() = " + multiResult.getColumnNames());
        for (Column<String> c : multiResult) {
            System.out.println(c.getName() + ": " + c.getStringValue());
        }
    }

}
