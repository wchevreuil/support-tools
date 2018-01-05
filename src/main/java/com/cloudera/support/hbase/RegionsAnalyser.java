package com.cloudera.support.hbase;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by wchevreuil on 27/12/2017.
 */
public class RegionsAnalyser {


  public static void main (String[] args) throws  Exception {

    String hbckPath = args[0];

    String hdfsList = args[1];

    String metaScan = args[2];

    List<Region> regions = getRegionsDetails(getProblematicRegions(hbckPath),
        metaScan);

    Collections.sort(regions);

    for(Region region : regions) {

      System.out.println(">>> " + region);

    }

    System.out.print("Total problematic regions: " + regions.size());

  }

  public static List<Region> getRegionsDetails(Set<String> regionsSet, String
      metaScan) throws
      Exception {

    BufferedReader reader = new BufferedReader(new InputStreamReader(new
        FileInputStream(metaScan)));

    String line = reader.readLine();

    List<Region> regions = new ArrayList<>();

    while (line != null) {

      if(line.indexOf("info:regioninfo")>0){

        String regionId = line.split("\\.")[1].split("\\.")[0];

        if(regionsSet.contains(regionId)){

          String startKey = line.substring(line.indexOf("STARTKEY => "),line
              .indexOf(", ENDKEY"));
          String endKey = line.substring(line.indexOf("ENDKEY => "), line
              .indexOf("}"));

          Region region = new Region(regionId, startKey, endKey);

          regions.add(region);

        }

      }

      line = reader.readLine();

    }

    return regions;
  }


  public static Set<String> getProblematicRegions (String hbck) throws
    Exception {

      BufferedReader reader = new BufferedReader(new InputStreamReader(new
          FileInputStream(hbck)));

      String line = reader.readLine();

      Set<String> regions = new HashSet<>();

      while (line != null) {

        if (line.indexOf("Multiple regions have the same startkey") > 0) {

          String region = line.split("\\.")[1].split("\\.")[0];

          regions.add(region);

        }

        if (line.indexOf("There is an overlap in the region chain.") > 0) {

          String[] split = line.split("and");

          String firstRegion = split[0].split("\\.")[1].split("\\.")[0];

          String secondRegion = split[1].split("\\.")[1].split("\\.")[0];

          regions.add(firstRegion);

          regions.add(secondRegion);

        }

        line = reader.readLine();

      }

      reader.close();

      return regions;

    }

  static class Region implements Comparable<Region>{

    String regionId;

    String startKey;

    String endKey;

    public Region(String id, String startKey, String endKey){

      this.regionId = id;

      this.startKey = startKey;

      this.endKey = endKey;

    }

    @Override public int compareTo(Region other) {

      if(this.startKey.compareTo(other.startKey)<0) {
        return -1;
      } else if(this.startKey.compareTo(other.startKey)==0) {
        return this.endKey.compareTo(other.endKey);
      }
      return 1;
    }

    @Override
    public String toString(){
      return this.regionId + ", startkey: " + startKey + ", endkey: " + endKey;
    }
  }



}

