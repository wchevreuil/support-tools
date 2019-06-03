package com.cloudera.support.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RegionMetaBuilder {
  private static final String HBASE_DATA_DIR = "/hbase/data/";
  private static final String HBASE_DEFAULT_NAMESPACE = "default/";
  private FileSystem fs;
  private Connection conn;

  public RegionMetaBuilder() throws IOException {
    Configuration config = HBaseConfiguration.create();
    this.fs = FileSystem.get(config);
    this.conn = ConnectionFactory.createConnection(config);
  }

  public static void main(String[] args) throws Exception {
    final RegionMetaBuilder metaBuilder = new RegionMetaBuilder();
    List<String> tables = metaBuilder.readTablesInputFile(args[0]);
    ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    final CountDownLatch countDownLatch = new CountDownLatch(tables.size());
    final List<String> encodedRegionNames = new ArrayList<>();
    for(String table : tables){
      executorService.submit(new Runnable() {
        @Override public void run() {
          try {
            System.out.println("running thread for " + table);
            List<Path> missingRegions = metaBuilder.findMissingRegionsInMETA(table);
            missingRegions.parallelStream().forEach(path -> {
              try {
                metaBuilder.putRegionInfoFromHdfsInMeta(path);
                encodedRegionNames.add(path.getName());
              } catch (Exception e) {
                e.printStackTrace();
              }
            });
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            countDownLatch.countDown();
          }
        }
      });
    }
    countDownLatch.await();
    executorService.shutdown();
    metaBuilder.printHbck2AssignsCommand(encodedRegionNames);
    metaBuilder.conn.close();
  }

  public List<String> readTablesInputFile(String tablesInputFile) throws Exception{
    final List<String> tablesRootDir = new ArrayList<>();
    try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(tablesInputFile)))){
      String currentTable = null;
      while((currentTable = reader.readLine())!=null){
        //formats table name to "/hbase/data/NAMESPACE/TABLE/"
        currentTable = currentTable.trim().split(" ")[0];
        System.out.println(currentTable);
        if(currentTable.indexOf(":")>0){
          currentTable = HBASE_DATA_DIR + currentTable.trim().replaceAll(":", "/");
        } else {
          currentTable = HBASE_DATA_DIR + HBASE_DEFAULT_NAMESPACE + currentTable.trim();
        };
        System.out.println("adding " + currentTable);
        tablesRootDir.add(currentTable);
      }
    }
    return tablesRootDir;
  }

  public List<Path> findMissingRegionsInMETA(String tableRootDir) throws Exception {
    final List<Path> missingRegions = new ArrayList<>();
    final FileStatus[] regionsDirs = fs.listStatus(new Path(tableRootDir));
    TableName tableName = tableRootDir.indexOf(HBASE_DEFAULT_NAMESPACE)>0 ?
      TableName.valueOf(tableRootDir.substring(tableRootDir.lastIndexOf("/")+1)) :
      TableName.valueOf(tableRootDir.substring(tableRootDir.lastIndexOf(HBASE_DATA_DIR) + HBASE_DATA_DIR.length())
        .replaceAll("/",":"));
    List<RegionInfo> regionInfos = MetaTableAccessor.
      getTableRegions(this.conn, tableName, true);
    for(final FileStatus regionDir : regionsDirs){
      if(!regionDir.getPath().getName().equals(".tabledesc")&&!regionDir.getPath().getName().equals(".tmp")) {
        System.out.println("looking for " + regionDir + " in META.");
        boolean foundInMeta = regionInfos.stream()
          .anyMatch(info -> info.getEncodedName().equals(regionDir.getPath().getName()));
        if (!foundInMeta) {
          System.out.println(regionDir + "is not in META.");
          missingRegions.add(regionDir.getPath());
        }
      }
    }
    return missingRegions;
  }

  public void putRegionInfoFromHdfsInMeta(Path region) throws Exception{
    RegionInfo info = HRegionFileSystem.loadRegionInfoFileContent(fs, region);
    MetaTableAccessor.addRegionToMeta(conn, info);
  }

  public void printHbck2AssignsCommand(List<String> regions) throws Exception {
    final StringBuilder builder = new StringBuilder();
    builder.append("assigns ");
    regions.forEach(region -> builder.append(region).append(" "));
    System.out.println("HBCK2 assigns command: " + builder.toString());
  }
}
