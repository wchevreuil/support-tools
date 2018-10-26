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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSink;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;


import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class TestWalEntrySplitterReplicationCoprocessor {

  private WalEntrySplitterReplicationCoprocessor coprocessor;
  private Connection mockedConnection;
  private Table mockedTable;
  private HRegionServer mockedRegionServer;
  private RegionServerCoprocessorEnvironment mockedEnvironment;
  private ReplicationSink mockedReplicationSink;
  private MetricsSink mockedMetrics;
  private static final String TABLE_NAME = "TEST_TBL";

  @Before
  public void setup() throws Exception {
    this.coprocessor = new WalEntrySplitterReplicationCoprocessor();
    this.mockedConnection = Mockito.<Connection>mock(Connection.class);
    this.coprocessor.sharedHtableCon = this.mockedConnection;
    this.mockedTable = mock(Table.class);
    this.mockedRegionServer = mock(HRegionServer.class);
    when(this.mockedConnection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(this.mockedTable);
    this.mockedEnvironment = mock(RegionServerCoprocessorEnvironment.class);
    when(this.mockedEnvironment.getRegionServerServices()).thenReturn(this.mockedRegionServer);
    this.mockedRegionServer.replicationSinkHandler = mock(Replication.class);
    this.mockedReplicationSink = mock(ReplicationSink.class);
    this.mockedMetrics = mock(MetricsSink.class);
    when(this.mockedReplicationSink.getSinkMetrics()).thenReturn(this.mockedMetrics);

    Field field = this.mockedRegionServer.replicationSinkHandler.getClass()
        .getDeclaredField("replicationSink");
    field.setAccessible(true);
    field.set(this.mockedRegionServer.replicationSinkHandler,this.mockedReplicationSink);

    field = this.mockedReplicationSink.getClass().getDeclaredField("totalReplicatedEdits");
    field.setAccessible(true);
    field.set(this.mockedReplicationSink, new AtomicLong(0));

  }

  @Test
  public void testStartCoprocessorValidEnvironment() throws IOException {
    this.coprocessor.start(this.mockedEnvironment);
    assertTrue(this.coprocessor.regionServer == this.mockedRegionServer);
  }

  @Test
  public void testStartCoprocessorInValidEnvironment() throws IOException {
    this.coprocessor.start(mock(CoprocessorEnvironment.class));
    assertNull(this.coprocessor.regionServer);
  }

  @Test
  public void testPreReplicateLogEntriesFewOps() throws Exception{
    this.coprocessor.start(this.mockedEnvironment);
    this.testPreReplicateLogEntriesBatches(10,1);
    verify(this.mockedMetrics, times(1)).applyBatch(10L);
  }

  @Test
  public void testPreReplicateLogEntriesMoreThan1kOps() throws Exception{
    this.coprocessor.start(this.mockedEnvironment);
    this.testPreReplicateLogEntriesBatches(1001,2);
    verify(this.mockedMetrics, times(1)).applyBatch(1000L);
    verify(this.mockedMetrics, times(1)).applyBatch(1L);
  }

  @Test
  public void testPreReplicateLogEntriesInvalidEnvironementMoreThan1kOps() throws Exception{
    this.coprocessor.start(mock(CoprocessorEnvironment.class));
    this.testPreReplicateLogEntriesBatches(1,1);
    verify(this.mockedMetrics, times(0)).applyBatch(anyLong());
  }

  private void testPreReplicateLogEntriesBatches(int numberOfOps, int expectedBatches) throws Exception {
    List<AdminProtos.WALEntry> entries = new ArrayList<>();
    AdminProtos.WALEntry mockedEntry = mock(AdminProtos.WALEntry.class);
    entries.add(mockedEntry);
    WALProtos.WALKey mockedKey = mock(WALProtos.WALKey.class);
    when(mockedEntry.getKey()).thenReturn(mockedKey);
    when(mockedKey.getTableName()).thenReturn(ByteString.copyFrom((TableName.valueOf(TABLE_NAME).getName())));
    when(mockedEntry.getAssociatedCellCount()).thenReturn(new Integer(numberOfOps));
    List<HBaseProtos.UUID> clusterIds = new ArrayList<HBaseProtos.UUID>();
    HBaseProtos.UUID mockedId = mock(HBaseProtos.UUID.class);
    when(mockedId.getLeastSigBits()).thenReturn(0L);
    when(mockedId.getMostSigBits()).thenReturn(0L);
    clusterIds.add(mockedId);
    when(mockedKey.getClusterIdsList()).thenReturn(clusterIds);
    CellScanner mockedScanner = mock(CellScanner.class);
    when(mockedScanner.advance()).thenReturn(true);

    KeyValue cell = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"), Bytes.toBytes("q"));
    when(mockedScanner.current()).thenReturn(cell);

    this.coprocessor.preReplicateLogEntries(null,
        Collections.unmodifiableList(entries),
        mockedScanner);
    verify(this.mockedTable, times(expectedBatches)).batch(anyListOf(Row.class), (Object[]) anyObject());

  }

}
