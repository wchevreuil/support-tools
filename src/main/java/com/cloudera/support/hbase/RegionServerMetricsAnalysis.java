package com.cloudera.support.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wchevreuil on 14/05/2018.
 */
public class RegionServerMetricsAnalysis {

  public static void main (String[] args) throws  Exception {

    RegionServerMetricsAnalysis analyser = new RegionServerMetricsAnalysis();

    analyser.memstoreSizePerTable(new File(args[0]));

  }

  public void memstoreSizePerTable(File metrics) throws  Exception {

    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(metrics)));

    String line = reader.readLine();

    Map<String,Long> memstoreSizePerTable = new HashMap<>();

    while(line!=null){

      if(line.indexOf("metric_memStoreSize")>=0){

        String table = line.split("_region_")[0];

        Long regionMemstoreSize = new Long(line.split(": ")[1].split(",")[0]);

        Long tableMemstoreSize =  memstoreSizePerTable.get(table);

        if(tableMemstoreSize == null){

          tableMemstoreSize = 0L;

        } else {

          tableMemstoreSize += regionMemstoreSize;

        }

        memstoreSizePerTable.put(table,tableMemstoreSize);

      }

      line = reader.readLine();
    }

    for(String table : memstoreSizePerTable.keySet()){

      System.out.println(table + " : " + memstoreSizePerTable.get(table));

    }

    reader.close();

  }

}
