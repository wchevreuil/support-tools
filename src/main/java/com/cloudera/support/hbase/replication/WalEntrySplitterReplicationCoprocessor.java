package com.cloudera.support.hbase.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Field;
import java.util.*;

public class WalEntrySplitterReplicationCoprocessor extends BaseRegionServerObserver {

    private static final Log LOG = LogFactory.getLog(WalEntrySplitterReplicationCoprocessor.class);

    private ReplicationSink replicationSink;

    private final Configuration conf  = HBaseConfiguration.create();

    private volatile Connection sharedHtableCon;

    private final Object sharedHtableConLock = new Object();

    public WalEntrySplitterReplicationCoprocessor() throws IOException {
        this.replicationSink = new ReplicationSink(HBaseConfiguration.create(),null);
    }

    public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                       List<AdminProtos.WALEntry> entries, CellScanner cells) throws IOException {


        try {
            List<AdminProtos.WALEntry> modifiableEntries = this.getModifiableList(entries);

            for (int i = 0; i < modifiableEntries.size(); i++) {

                AdminProtos.WALEntry entry = modifiableEntries.get(i);

                LOG.info("replication entry cell count: " + entry.getAssociatedCellCount());

                try {
                    long totalReplicated = 0;
                    // Map of table => list of Rows, grouped by cluster id, we only want to flushCommits once per
                    // invocation of this method per table and cluster id.
                    Map<TableName, Map<List<UUID>, List<Row>>> rowMap =
                            new TreeMap<TableName, Map<List<UUID>, List<Row>>>();

                    TableName table =
                            TableName.valueOf(entry.getKey().getTableName().toByteArray());

                    Cell previousCell = null;

                    Mutation m = null;

                    int count = entry.getAssociatedCellCount();

                    for (int j = 0; j < count; j++) {
                        // Throw index out of bounds if our cell count is off
                        if (!cells.advance()) {

                            throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + j);
                        }

                        Cell cell = cells.current();

                        if (isNewRowOrType(previousCell, cell) || rowMap.isEmpty()) {
                            // Create new mutation
                            m = newMutation(cell,entry,rowMap,table);

                        }

                        if (CellUtil.isDelete(cell)) {
                            ((Delete) m).addDeleteMarker(cell);
                        } else {
                            ((Put) m).add(cell);
                        }


                        previousCell = cell;

                        totalReplicated++;

                        //here we avoid batch mutating more than 1,0000 cells at once
                        if (totalReplicated > 1_000) {
                            LOG.info("batching 1,000 cells from entry... ");
                            for (Map.Entry<TableName, Map<List<UUID>, List<Row>>> edit : rowMap.entrySet()) {
                                batch(edit.getKey(), edit.getValue().values());
                            }

                            rowMap.clear();

                            totalReplicated = 0;

                        }
                    }
                    for (Map.Entry<TableName, Map<List<UUID>, List<Row>>> edit : rowMap.entrySet()) {
                        batch(edit.getKey(), edit.getValue().values());
                    }
//                int size = entries.size();
//                this..setAgeOfLastAppliedOp(entries.get(size - 1).getKey().getWriteTime());
//                this.metrics.applyBatch(size);
//                this.totalReplicatedEdits.addAndGet(totalReplicated);
                    modifiableEntries.remove(i);
                } catch (IOException ex) {
                    LOG.error("Unable to accept edit because:", ex);
                    throw ex;
                }

            }
        }catch(Exception e){
            LOG.error("Unexpected error: ", e);
        }

    }

    private Mutation newMutation(Cell cell,
                                 AdminProtos.WALEntry entry,
                                 Map<TableName, Map<List<UUID>, List<Row>>> rowMap,
                                 TableName table){

        Mutation m = CellUtil.isDelete(cell) ?
                new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) :
                new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());

        List<UUID> clusterIds = new ArrayList<UUID>();

        for (HBaseProtos.UUID clusterId : entry.getKey().getClusterIdsList()) {
            clusterIds.add(toUUID(clusterId));
        }

        m.setClusterIds(clusterIds);

        addToHashMultiMap(rowMap, table, clusterIds, m);

        return m;
    }

    private List<AdminProtos.WALEntry> getModifiableList(List<AdminProtos.WALEntry> entries) throws Exception {


        for(Field f: entries.getClass().getSuperclass().getDeclaredFields()){
            LOG.info("field: " + f.getName());
        }

        LOG.info(" class: " + entries.getClass().getSuperclass().getName());

        Field field = entries.getClass().getSuperclass().getDeclaredField("list");
        field.setAccessible(true);
        return (List<AdminProtos.WALEntry>) field.get(entries);

    }

    private boolean isNewRowOrType(final Cell previousCell, final Cell cell) {
        return previousCell == null || previousCell.getTypeByte() != cell.getTypeByte() ||
                !CellUtil.matchingRow(previousCell, cell);
    }

    private <K1, K2, V> List<V> addToHashMultiMap(Map<K1, Map<K2,List<V>>> map, K1 key1, K2 key2, V value) {
        Map<K2,List<V>> innerMap = map.get(key1);
        if (innerMap == null) {
            innerMap = new HashMap<K2, List<V>>();
            map.put(key1, innerMap);
        }
        List<V> values = innerMap.get(key2);
        if (values == null) {
            values = new ArrayList<V>();
            innerMap.put(key2, values);
        }
        values.add(value);
        return values;
    }

    private java.util.UUID toUUID(final HBaseProtos.UUID uuid) {
        return new java.util.UUID(uuid.getMostSigBits(), uuid.getLeastSigBits());
    }

    protected void batch(TableName tableName, Collection<List<Row>> allRows) throws IOException {
        if (allRows.isEmpty()) {
            return;
        }
        Table table = null;
        try {
            // See https://en.wikipedia.org/wiki/Double-checked_locking
            Connection connection = this.sharedHtableCon;
            if (connection == null) {
                synchronized (sharedHtableConLock) {
                    connection = this.sharedHtableCon;
                    if (connection == null) {
                        connection = this.sharedHtableCon = ConnectionFactory.createConnection(this.conf);
                    }
                }
            }
            table = connection.getTable(tableName);
            for (List<Row> rows : allRows) {
                table.batch(rows);
            }
        } catch (InterruptedException ix) {
            throw (InterruptedIOException)new InterruptedIOException().initCause(ix);
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }
}
