package org.jauntsy.grinder.panama.contrib.snapshot;

import org.jauntsy.grinder.panama.api.Dbv;
import org.jauntsy.grinder.panama.base.BaseSnapshotAdapter;
import com.google.common.cache.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * User: ebishop
 * Date: 12/29/12
 * Time: 12:49 PM
 */
public class HbaseSnapshotAdapter extends BaseSnapshotAdapter {

    public static final byte[] SNAPSHOT_QUALIFIER = Bytes.toBytes("snapshot");
    public static final byte[] DBV_QUALIFIER = Bytes.toBytes("dbv");


    private transient LoadingCache<String, HTable> cache;
    private final Configuration conf;

    public HbaseSnapshotAdapter(Configuration conf) {
        this.conf = conf;
        this.cache = CacheBuilder.newBuilder()
                .refreshAfterWrite(5, TimeUnit.MINUTES)
                .removalListener(new RemovalListener<String, HTable>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, HTable> notification) {
                        try {
                            notification.getValue().close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                })
                .build(new CacheLoader<String, HTable>() {
                    @Override
                    public HTable load(String table) throws Exception {
                        return new HTable(HbaseSnapshotAdapter.this.conf, table);
                    }
                });
    }

    @Override
    public void close() {
        cache.cleanUp();
    }

    @Override
    public List<Dbv> getBatch(String table, List<List> ids) {
        try {
        synchronized (cache) {
            try {
                HTable hTable = cache.get(table);
                List<Get> gets = new ArrayList<Get>();
                for (List id : ids) {
                    Get get = new Get(buildId(id));
                    gets.add(get);
                }
                List<Dbv> ret = new ArrayList<Dbv>();
                Result[] results = hTable.get(gets);
                for (Result result : results) {
                    byte[] value = result.getValue(SNAPSHOT_QUALIFIER, DBV_QUALIFIER);
                    ret.add(value == null ? null : bytesToDbv(value));
                }
                return ret;
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private Dbv bytesToDbv(byte[] value) {
        throw new UnsupportedOperationException();
    }

    private byte[] dbvToBytes(Dbv dbv) {
        throw new UnsupportedOperationException();
    }

    private byte[] buildId(List id) {
            return Bytes.toBytes(JSONValue.toJSONString(id));
    }

    @Override
    public void putBatch(String table, List<List> ids, List<Dbv> values) {
        throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
    }

}
