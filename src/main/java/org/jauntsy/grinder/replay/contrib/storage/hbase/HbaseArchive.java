package org.jauntsy.grinder.replay.contrib.storage.hbase;

import backtype.storm.utils.Utils;
import org.jauntsy.grinder.replay.base.BaseArchiveClient;
import org.jauntsy.nice.Time;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 12/28/12
 * Time: 3:30 PM
 */
public class HbaseArchive extends BaseArchiveClient {

    public static final byte[] FAMILY_QUALIFIER = Bytes.toBytes("messages");
    public static final byte[] MESSAGE_QUALIFIER = Bytes.toBytes("message");
    public static final byte[] OFFSET_QUALIFIER = Bytes.toBytes("offset");

    private final Configuration configuration;
    private final String tableName;

    private final HBaseAdmin admin;
    private final HTable table;

    private final Map<String,Partition> partitions;

    public HbaseArchive(Configuration configuration, String tableName) throws IOException {
        this.configuration = configuration;
        this.tableName = tableName;

        this.admin = new HBaseAdmin(configuration);
        if (!this.admin.tableExists(tableName)) {
            try {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                tableDescriptor.addFamily(new HColumnDescriptor(FAMILY_QUALIFIER));
                this.admin.createTable(tableDescriptor);
            } catch(Exception ignore) {

            }
        }

        this.table = new HTable(configuration, tableName);

        this.partitions = new HashMap<String, Partition>();
    }

    @Override
    public Partition getPartition(String topic, String host, int port, int partition) {
        return getOrCreatePartition(topic, host, port, partition);
    }

    private Partition getOrCreatePartition(String topic, String host, int port, int partition) {
        String seq = topic + "_" + host + "_" + port + "_" + partition;
        Partition p = partitions.get(seq);
        if (p == null) {
            p = new PartitionImpl(seq);
            partitions.put(seq, p);
        }
        return p;
    }

    @Override
    public void close() {
        try { table.close(); } catch(Exception ignore) {}
        try { admin.close(); } catch(Exception ignore) {}
    }

    private class PartitionImpl implements Partition {

        private final String prefix;

        public PartitionImpl(String seq) {
            this.prefix = seq + "/";
        }

        @Override
        public List<MessageAndOffset> fetch(long offset, int maxSize) {
            try {
                long start = Time.now();
                List<MessageAndOffset> ret = new ArrayList<MessageAndOffset>();
                String startRow = buildRowKey(offset);
                String endRow = buildRowKey(Long.MAX_VALUE);
                System.out.println("Fetch startRow: " + startRow + ", endRow: " + endRow);
                Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
                scan.addColumn(FAMILY_QUALIFIER, MESSAGE_QUALIFIER);
                scan.addColumn(FAMILY_QUALIFIER, OFFSET_QUALIFIER);
                scan.setBatch(1000);
                scan.setCacheBlocks(true);
                scan.setCaching(10000);
                ResultScanner scanner = table.getScanner(scan);
                int totalSize = 0;
                for (Result result : scanner) {
                    if (result != null && !result.isEmpty()) {
                        byte[] messageBytes = result.getValue(FAMILY_QUALIFIER, MESSAGE_QUALIFIER);
                        byte[] offsetBytes = result.getValue(FAMILY_QUALIFIER, OFFSET_QUALIFIER);
                        totalSize += messageBytes.length;
                        if (totalSize <= maxSize) {
                            ret.add(new MessageAndOffset(new Message(messageBytes), Bytes.toLong(offsetBytes)));
                        } else {
                            break;
                        }
                    }
                }
                scanner.close();
                System.out.println("Returning " + ret.size() + " results in " + Time.since(start) + " ms");
                return ret;
            } catch(Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public long[] getOffsetsBefore(long time, int maxNumOffsets) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }

        @Override
        public void send(long offset, MessageAndOffset messageAndOffset) {
            try {
                String key = buildRowKey(offset);
                Put put = new Put(Bytes.toBytes(key));
                put.add(FAMILY_QUALIFIER, MESSAGE_QUALIFIER, Utils.toByteArray(messageAndOffset.message().payload()));
                put.add(FAMILY_QUALIFIER, OFFSET_QUALIFIER, Bytes.toBytes(messageAndOffset.offset()));
                table.put(put);
            } catch(Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        private String buildRowKey(long offset) {
            String soffset = String.valueOf(offset);
            StringBuilder sb =  new StringBuilder(prefix);
            for (int i = 20 - soffset.length(); i > 0; i--)
                sb.append('0');
            sb.append(soffset);
            return sb.toString();
        }

        @Override
        public void send(Map<Long, MessageAndOffset> messagesAndOffsets) {
            throw new UnsupportedOperationException(); //To change body of implemented methods use File | Settings | File Templates | Code | Implemented Method Body.
        }
    }

}
