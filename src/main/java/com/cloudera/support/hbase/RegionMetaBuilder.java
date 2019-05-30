package com.cloudera.support.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class RegionMetaBuilder {

  public static void main(String[] args) throws IOException {
    Configuration config = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(config);
    RegionInfo info = HRegionFileSystem.loadRegionInfoFileContent(fs, new Path(args[0]));
    System.out.println(info.getEncodedName());
    System.out.println(info.getRegionId());
    System.out.println(Bytes.toString(info.getStartKey()));
    System.out.println(Bytes.toString(info.getEndKey()));
    System.out.println(info.isOffline());
    System.out.println(info.isSplit());
    System.out.println(info.isSplitParent());
    Connection conn = ConnectionFactory.createConnection(config);
    MetaTableAccessor.addRegionToMeta(conn, info);
    conn.close();
    System.out.println("Finished inserting region back into meta.");
  }
}
