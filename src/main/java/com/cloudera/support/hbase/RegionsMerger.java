package com.cloudera.support.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class RegionsMerger {

  public static void Main(String[] args) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException, InterruptedException {

    Configuration config = HBaseConfiguration.create();

    HBaseAdmin admin = new HBaseAdmin(config);

    String tblName = args[0];

    int numRegions = Integer.parseInt(args[1]);

    int counter = 0;

    List<HRegionInfo> regions = admin.getTableRegions(Bytes.toBytes(tblName));

    while (regions.size() > numRegions) {

      System.out.println("iteration: " + counter);

      HRegionInfo previous = null;

      for (HRegionInfo current : regions) {

        if (!current.isSplit()) {

          if (previous == null) {

            previous = current;

          } else {

            if (HRegionInfo.areAdjacent(previous, current)) {

              admin.mergeRegions(current.getEncodedNameAsBytes(), previous.getEncodedNameAsBytes(),
                true);

              System.out.println("merged adjacent regions " + current.getEncodedName() + " and "
                  + previous.getEncodedName());

              previous = null;

            } else {

              System.out.println("regions " + current.getEncodedName() + " and "
                  + previous.getEncodedName() + " are not adjacent");

            }

          }

        }

      }

      counter++;

      System.out.println("sleeping for 2 seconds before next iteration...");

      Thread.sleep(2000);

      regions = admin.getTableRegions(Bytes.toBytes(tblName));

    }

  }

}
