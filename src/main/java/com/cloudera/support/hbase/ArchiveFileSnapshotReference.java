package com.cloudera.support.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.master.CatalogJanitor;
import org.apache.hadoop.hbase.master.ClusterSchema;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MasterWalManager;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.master.snapshot.SnapshotHFileCleaner;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

/**
 * This program allows to inspect hfiles on specified path,
 * checking if the given hfiles are still referenced by any existing snapshot.
 * 
 * The main goal is to be used as an alternative tool to HBase's own 
 * archive cleaning feature, mainly on situations where archive folder
 * has grown too large, and HMaster's cleaner thread is taking too long
 * to reclaim HDFS space, whilst using this tool, it's possible to specify
 * specific subdirs within archive directory, for example, older dirs or those
 * related to a recently deleted snapshot, which are more likely to have 
 * unreferenced files.
 * 
 * It traverses the specified path recursively. 
 * It should be only passed subdirectories of hbase archive folder. 
 * It currently does not not perform any validation on that, passing paths for 
 * hbase tables (i.e. "/hbase/data/default/table_name") may lead to 
 * unpredicted results and has not been tested. Please don't do that.
 * 
 * Usage:
 *
 * com.cloudera.support.hbase.ArchiveFileSnapshotReference ARCHIVE_SUBDIR [delete]
 * 
 * The only required parameter is the archive subdir. In this mode, 
 * it will go through the informed directry structure, 
 * checking hfiles that can be deleted. At the end a summary of how much files
 * would be deleted and hdfs space would be released is printed.
 * 
 * Passing optional "delete" flag as second parameter would cause the unreferenced
 * files to be deleted, releasing hdfs space.
 */
public class ArchiveFileSnapshotReference {

  private static SnapshotManager snapshotManager = new SnapshotManager();

  private SnapshotHFileCleaner fileCleaner = new SnapshotHFileCleaner();

  private FileSystem fs;

  public static void main(String[] args) throws Exception {

    ArchiveFileSnapshotReference references = new ArchiveFileSnapshotReference();

    references.fs = FileSystem.get(new Configuration());

    Map<String, Object> params = new HashMap<>();

    params.put(HMaster.MASTER, new MasterServiceFacade());

    references.fileCleaner.init(params);

    references.fileCleaner.setConf(HBaseConfiguration.create());

    List<FileStatus> filesToCheck = new ArrayList<FileStatus>();

    references.checkArchiveFolder(new Path(args[0]), filesToCheck);

    int countDeletable = 0;
    long totalBytesReleaseable = 0;

    for (FileStatus f : references.fileCleaner.getDeletableFiles(filesToCheck)) {

      countDeletable++;

      totalBytesReleaseable += f.getLen();

      System.out.println(
        "file " + f.getPath() + " is deletable, and would release " + f.getLen() + " bytes");

      if (args.length == 2 && args[1].equals("delete")) {
        System.out.println("file " + f.getPath() + " will be deleted now");
        references.fs.delete(f.getPath(), false);
        System.out.println("deleted file " + f.getPath() + " of size " + f.getLen() + " bytes");
      }

    }

    if (countDeletable == 0) {
      System.out.println("Finished iterating. No file under " + args[0] + " is deletable");
    } else {
      System.out.println("Total deletable files under " + args[0] + ": " + countDeletable);
      if (args.length == 2 && args[1].equals("delete")) {
        System.out
            .println("Total space released with deletion: " + totalBytesReleaseable + " bytes");
      } else {
        System.out.println("Space to be released: " + totalBytesReleaseable + " bytes");
      }
    }

    references.fileCleaner.stop("closing the utility program");

    references.fs.close();

  }

  public void checkArchiveFolder(Path root, List<FileStatus> files) throws Exception {

    for (FileStatus f : fs.listStatus(root)) {

      if (f.isDirectory()) {

        checkArchiveFolder(f.getPath(), files);

      } else {

        files.add(f);

      }

    }
  }

  public static class MasterServiceFacade implements MasterServices {

    @Override public SnapshotManager getSnapshotManager() {
      return snapshotManager;
    }

    @Override public MasterProcedureManagerHost getMasterProcedureManagerHost() {
      return null;
    }

    @Override public ClusterSchema getClusterSchema() {
      return null;
    }

    @Override public AssignmentManager getAssignmentManager() {
      return null;
    }

    @Override public MasterFileSystem getMasterFileSystem() {
      return null;
    }

    @Override public MasterWalManager getMasterWalManager() {
      return null;
    }

    @Override public ServerManager getServerManager() {
      return null;
    }

    @Override public ExecutorService getExecutorService() {
      return null;
    }

    @Override public TableStateManager getTableStateManager() {
      return null;
    }

    @Override public MasterCoprocessorHost getMasterCoprocessorHost() {
      return null;
    }

    @Override public MasterQuotaManager getMasterQuotaManager() {
      return null;
    }

    @Override public RegionNormalizer getRegionNormalizer() {
      return null;
    }

    @Override public CatalogJanitor getCatalogJanitor() {
      return null;
    }

    @Override public ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
      return null;
    }

    @Override public ProcedureEvent<?> getInitializedEvent() {
      return null;
    }

    @Override public MetricsMaster getMasterMetrics() {
      return null;
    }

    @Override public void checkTableModifiable(TableName tableName)
        throws IOException, TableNotFoundException, TableNotDisabledException {

    }

    @Override
    public long createTable(TableDescriptor tableDescriptor, byte[][] bytes, long l, long l1)
        throws IOException {
      return 0;
    }

    @Override public long createSystemTable(TableDescriptor tableDescriptor) throws IOException {
      return 0;
    }

    @Override public long deleteTable(TableName tableName, long l, long l1) throws IOException {
      return 0;
    }

    @Override public long truncateTable(TableName tableName, boolean b, long l, long l1)
        throws IOException {
      return 0;
    }

    @Override
    public long modifyTable(TableName tableName, TableDescriptor tableDescriptor, long l, long l1)
        throws IOException {
      return 0;
    }

    @Override public long enableTable(TableName tableName, long l, long l1) throws IOException {
      return 0;
    }

    @Override public long disableTable(TableName tableName, long l, long l1) throws IOException {
      return 0;
    }

    @Override
    public long addColumn(TableName tableName, ColumnFamilyDescriptor columnFamilyDescriptor,
    long l, long l1) throws IOException {
      return 0;
    }

    @Override
    public long modifyColumn(TableName tableName, ColumnFamilyDescriptor columnFamilyDescriptor,
    long l, long l1) throws IOException {
      return 0;
    }

    @Override public long deleteColumn(TableName tableName, byte[] bytes, long l, long l1)
        throws IOException {
      return 0;
    }

    @Override public long mergeRegions(RegionInfo[] regionInfos, boolean b, long l, long l1)
        throws IOException {
      return 0;
    }

    @Override public long splitRegion(RegionInfo regionInfo, byte[] bytes, long l, long l1)
        throws IOException {
      return 0;
    }

    @Override public TableDescriptors getTableDescriptors() {
      return null;
    }

    @Override public boolean registerService(Service service) {
      return false;
    }

    @Override public boolean isActiveMaster() {
      return false;
    }

    @Override public boolean isInitialized() {
      return false;
    }

    @Override public boolean isInMaintenanceMode() {
      return false;
    }

    @Override public boolean abortProcedure(long l, boolean b) throws IOException {
      return false;
    }

    @Override public List<Procedure<?>> getProcedures() throws IOException {
      return null;
    }

    @Override public List<LockedResource> getLocks() throws IOException {
      return null;
    }

    @Override public List<TableDescriptor> listTableDescriptorsByNamespace(String s)
        throws IOException {
      return null;
    }

    @Override public List<TableName> listTableNamesByNamespace(String s) throws IOException {
      return null;
    }

    @Override public long getLastMajorCompactionTimestamp(TableName tableName)
        throws IOException {
      return 0;
    }

    @Override public long getLastMajorCompactionTimestampForRegion(byte[] bytes)
        throws IOException {
      return 0;
    }

    @Override public LoadBalancer getLoadBalancer() {
      return null;
    }

    @Override public boolean isSplitOrMergeEnabled(MasterSwitchType masterSwitchType) {
      return false;
    }

    @Override public FavoredNodesManager getFavoredNodesManager() {
      return null;
    }

    @Override
    public long addReplicationPeer(String s, ReplicationPeerConfig replicationPeerConfig,
    boolean b) throws ReplicationException, IOException {
      return 0;
    }

    @Override public long removeReplicationPeer(String s)
        throws ReplicationException, IOException {
      return 0;
    }

    @Override public long enableReplicationPeer(String s)
        throws ReplicationException, IOException {
      return 0;
    }

    @Override public long disableReplicationPeer(String s)
        throws ReplicationException, IOException {
      return 0;
    }

    @Override public ReplicationPeerConfig getReplicationPeerConfig(String s)
        throws ReplicationException, IOException {
      return null;
    }

    @Override public ReplicationPeerManager getReplicationPeerManager() {
      return null;
    }

    @Override
    public long updateReplicationPeerConfig(String s, ReplicationPeerConfig replicationPeerConfig)
        throws ReplicationException, IOException {
      return 0;
    }

    @Override public List<ReplicationPeerDescription> listReplicationPeers(String s)
        throws ReplicationException, IOException {
      return null;
    }

    @Override public LockManager getLockManager() {
      return null;
    }

    @Override public String getRegionServerVersion(ServerName serverName) {
      return null;
    }

    @Override public void checkIfShouldMoveSystemRegionAsync() {

    }

    @Override public String getClientIdAuditPrefix() {
      return null;
    }

    @Override public boolean isClusterUp() {
      return false;
    }

    @Override public Configuration getConfiguration() {
      return null;
    }

    @Override public ZKWatcher getZooKeeper() {
      return null;
    }

    @Override public Connection getConnection() {
      return null;
    }

    @Override public Connection createConnection(Configuration configuration) throws IOException {
      return null;
    }

    @Override public ClusterConnection getClusterConnection() {
      return null;
    }

    @Override public MetaTableLocator getMetaTableLocator() {
      return null;
    }

    @Override public ServerName getServerName() {
      return null;
    }

    @Override public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }

    @Override public ChoreService getChoreService() {
      return null;
    }

    @Override public void abort(String why, Throwable e) {

    }

    @Override public boolean isAborted() {
      return false;
    }

    @Override public void stop(String why) {

    }

    @Override public boolean isStopped() {
      return false;
    }
  }

}
