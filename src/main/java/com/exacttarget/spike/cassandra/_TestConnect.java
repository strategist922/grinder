package com.exacttarget.spike.cassandra;

import org.jauntsy.grinder.karma.mapred.Benchmark;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * User: ebishop
 * Date: 1/24/13
 * Time: 3:41 PM
 */
public class _TestConnect {
    public static void main(String[] args) throws ConnectionException {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forKeyspace("DEMO")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.NONE)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnection")
                        .setPort(9160)
                        .setMaxConnsPerHost(4)
                        .setSeeds("127.0.0.1:9160")
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace keyspace = context.getEntity();

        ColumnFamily<String,String> CF_USERS = new ColumnFamily<String, String>(
                "Users",
                StringSerializer.get(),
                StringSerializer.get()
        );


        Benchmark b = new Benchmark("readNWrites");
        while (true) {
            Benchmark.Start start = b.start();
            int key = (int)(10 * Math.random());
            double random = Math.random();
            if (random > 0.5) {
                MutationBatch m = keyspace.prepareMutationBatch();
                m
                        .withRow(CF_USERS, String.valueOf(key))
                        .putColumn("col", String.valueOf(random));
                m.execute();
                Column<String> col = keyspace.prepareQuery(CF_USERS).getKey(String.valueOf(key)).withColumnSlice("col").execute().getResult().iterator().next();
                if (!String.valueOf(random).equals(col.getStringValue()))
                    throw new IllegalStateException("Expected: " + random + ", got: " + col.getStringValue());
            } else {
                MutationBatch m = keyspace.prepareMutationBatch();
                m
                        .withRow(CF_USERS, String.valueOf(key))
                        .deleteColumn("col");
                m.execute();
                OperationResult<ColumnList<String>> col1 = keyspace.prepareQuery(CF_USERS).getKey(String.valueOf(key)).withColumnSlice("col").execute();
                if (col1.getResult().size() != 0)
                    throw new IllegalStateException("Expected: null, got: " + col1.getResult());
            }
            start.end();
            if (false)
                break;
        }

        try {
            OperationResult<ColumnList<String>> result = keyspace.prepareQuery(CF_USERS).getKey("1234").execute();
            ColumnList<String> columns = result.getResult();
            for (Column<String> column : columns) {
                System.out.format("%s: %s\n", column.getName(), column.getStringValue());
            }
        } catch (ConnectionException e) {
            e.printStackTrace();
        }

        context.shutdown();
    }
}
