/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class WalEntrySplitterReplicationCoprocessor extends BaseRegionServerObserver {

  private static final Log LOG = LogFactory.getLog(WalEntrySplitterReplicationCoprocessor.class);

  public static final String MAX_OPS_PER_BATCH =
      "hbase.support.coprocessor.walentrysplitter.max_ops";

  private final int maxOps;

  HRegionServer regionServer;

  ReplicationSink replicationSink;

  AtomicLong totalReplicatedEdits;

  final Configuration conf;

  volatile Connection sharedHtableCon;

  private final Object sharedHtableConLock = new Object();

  public WalEntrySplitterReplicationCoprocessor() throws IOException {
    this.conf = HBaseConfiguration.create();
    //here we avoid batch mutating more than 1,0000 cells at once
    this.maxOps = conf.getInt(MAX_OPS_PER_BATCH, 1000);
    LOG.info("instantiated WalEntrySplitterReplicationCoprocessor.");
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if(env instanceof RegionServerCoprocessorEnvironment){
      RegionServerServices serverServices = ((RegionServerCoprocessorEnvironment)env)
          .getRegionServerServices();
      LOG.info("Retrieved RegionServerServices instance: " + serverServices);
      if(serverServices instanceof HRegionServer){
        this.regionServer = (HRegionServer)serverServices;
      } else {
        LOG.warn("RegionServerServices not an instance of HRegionServer. "
            + "This may cause replication metrics to be inaccurate. ");
      }
    } else {
      LOG.warn("CoprocessorEnvironment not an instance of RegionServerCoprocessorEnvironment."
          + " Could not get reference for ReplicationSink. "
          + "This may cause replication metrics to be inaccurate. ");
    }
    LOG.info("finished loading WalEntrySplitterReplicationCoprocessor.");
  }


  private synchronized void loadSinkService(){
    if(this.replicationSink==null && this.regionServer!=null) {
      ReplicationSinkService sinkService = this.regionServer.replicationSinkHandler;
      LOG.info("Retrieved ReplicationSinkService instance: " + sinkService);
      if (sinkService instanceof Replication) {
        try {
          Field field = sinkService.getClass().getDeclaredField("replicationSink");
          field.setAccessible(true);
          this.replicationSink = (ReplicationSink) field.get(sinkService);
          try {
            field = this.replicationSink.getClass().getDeclaredField("totalReplicatedEdits");
            field.setAccessible(true);
            this.totalReplicatedEdits = (AtomicLong) field.get(this.replicationSink);
          } catch (Exception e) {
            LOG.warn("Could not get reference to totalReplicatedEdits inside ReplicationSink "
                + "instance. This may cause replication metrics to be inaccurate. ", e);
          }
        } catch (Exception e) {
          LOG.warn("Could not get reference to ReplicationSink instance. "
              + "This may cause replication metrics to be inaccurate. ", e);
        }
      } else {
        LOG.warn("sink service not an instance of Replication. "
            + "This may cause replication metrics to be inaccurate. ");
      }
    }
  }

  @Override
  public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      List<AdminProtos.WALEntry> entries, CellScanner cells) throws IOException {
    loadSinkService();
    try {
      List<AdminProtos.WALEntry> modifiableEntries = this.getModifiableList(entries);
      for (int i = 0; i < modifiableEntries.size(); i++) {
        AdminProtos.WALEntry entry = modifiableEntries.get(i);
        LOG.debug("replication entry cell count: " + entry.getAssociatedCellCount());
        try {
          long totalReplicated = 0;
          // Map of table => list of Rows, grouped by cluster id, we only want to flushCommits once per
          // invocation of this method per table and cluster id.
          Map<TableName, Map<List<UUID>, List<Row>>> rowMap =
              new TreeMap<TableName, Map<List<UUID>, List<Row>>>();
          TableName table = TableName.valueOf(entry.getKey().getTableName().toByteArray());
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
              m = newMutation(cell, entry, rowMap, table);
            }
            if (CellUtil.isDelete(cell)) {
              ((Delete) m).addDeleteMarker(cell);
            } else {
              ((Put) m).add(cell);
            }
            previousCell = cell;
            totalReplicated++;
            if (totalReplicated >= this.maxOps) {
              LOG.trace("batching " + this.maxOps + " cells from entry... ");
              for (Map.Entry<TableName, Map<List<UUID>, List<Row>>> edit : rowMap.entrySet()) {
                batch(edit.getKey(), edit.getValue().values());
              }
              updateMetrics(entry, totalReplicated);
              //ensures same OPs will not get re-batched on for loop of line #131
              rowMap.clear();
              totalReplicated = 0;
            }
          }
          for (Map.Entry<TableName, Map<List<UUID>, List<Row>>> edit : rowMap.entrySet()) {
            batch(edit.getKey(), edit.getValue().values());
          }
          if(totalReplicated>0) {
            updateMetrics(entry, totalReplicated);
          }
          modifiableEntries.remove(i);
        } catch (IOException ex) {
          LOG.error("Unable to accept edit because:", ex);
          throw ex;
        }
      }
      LOG.debug("finished pre-replicate call for " + entries.size() + " entries.");
    } catch (Exception e) {
      LOG.error("Unexpected error: ", e);
    }
  }

  private void updateMetrics(AdminProtos.WALEntry entry, long totalReplicated) {
    if(this.replicationSink!=null) {
      this.replicationSink.getSinkMetrics().setAgeOfLastAppliedOp(entry.getKey().getWriteTime());
      this.replicationSink.getSinkMetrics().applyBatch(totalReplicated);
    }else{
      LOG.info("Could not update replication sink metrics with getAgeOfLastAppliedOp: "
          + entry.getKey().getWriteTime() + ", applied batch ops: " + totalReplicated);
    }
    if(this.totalReplicatedEdits!=null) {
      this.totalReplicatedEdits.addAndGet(totalReplicated);
    }else{
      LOG.info("Could not update replication stats with extra replicated edits: "
          + totalReplicated);
    }
  }

  private Mutation newMutation(Cell cell, AdminProtos.WALEntry entry,
      Map<TableName, Map<List<UUID>, List<Row>>> rowMap, TableName table) {

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

  // Kind of a hack, but can't see other way: We need to remove entries batched by coprocessor
  // from original list, but the passed list is an unmodifiable collection,
  // so we need to do reflection to actually get the modifiable collection.
  private List<AdminProtos.WALEntry> getModifiableList(List<AdminProtos.WALEntry> entries)
      throws Exception {
    Field field = entries.getClass().getSuperclass().getDeclaredField("list");
    field.setAccessible(true);
    return (List<AdminProtos.WALEntry>) field.get(entries);
  }

  private boolean isNewRowOrType(final Cell previousCell, final Cell cell) {
    return previousCell == null || previousCell.getTypeByte() != cell.getTypeByte() || !CellUtil
        .matchingRow(previousCell, cell);
  }

  private <K1, K2, V> List<V> addToHashMultiMap(Map<K1, Map<K2, List<V>>> map, K1 key1, K2 key2,
      V value) {
    Map<K2, List<V>> innerMap = map.get(key1);
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

  private UUID toUUID(final HBaseProtos.UUID uuid) {
    return new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits());
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
        table.batch(rows, new Object[rows.size()]);
      }
    } catch (InterruptedException ix) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(ix);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }
}
