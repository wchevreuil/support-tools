package com.cloudera.support.hbase;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.io.File;
import java.text.DecimalFormat;

/**
 * Created by wchevreuil on 27/12/2017.
 *
 * Util tool to analyse regions info from outputs of meta scan, hbase dir
 * listing and hbck details.
 *
 * It produces a report of regions size. If an hbck output is passed and
 * there are inconsistent regions, it will only produce reports for these
 * inconsistent regions.
 *
 */
public class RegionsAnalyser {


  public static void main (String[] args) throws  Exception {

    String hdfsList = args[0];

    String metaScan = args[1];

    String hbckPath = null;

    if(args.length ==3) {

      hbckPath = args[2];

    }

    Map<String, TableStats> tableStatsMap = new HashMap<>();

    List<Region> regions = getRegionsDetails(hbckPath
            == null ? null : getProblematicRegions
            (hbckPath),
        metaScan, hdfsList, tableStatsMap);

    Collections.sort(regions);

    for(Region region : regions) {

      System.out.println(">>> " + region);

    }

    for(String tableName : tableStatsMap.keySet()){

      System.out.println("Stats for table " + tableName + ": ");
      System.out.println("Total size: " + tableStatsMap.get(tableName).tableTotalSize);
      System.out.println("Regions count: " + tableStatsMap.get(tableName).regionCount);
      System.out.println("Smallest region size: " + tableStatsMap.get
          (tableName).smallestRegionSize);
      System.out.println("Largest region size: " + tableStatsMap.get(tableName)
          .largestRegionSize);
      double totalSize = tableStatsMap.get(tableName).tableTotalSize;
      System.out.println("Avg region size: " + totalSize / tableStatsMap.get
          (tableName).regionCount);

    }


    if(hbckPath != null) {

      System.out.print("Total problematic regions: " + regions.size());

    }

  }

  public static List<Region> getRegionsDetails(Set<String> regionsSet, String
      metaScan, String hdfsList, Map<String, TableStats> tableStatsMap) throws
      Exception {

    BufferedReader reader = new BufferedReader(new InputStreamReader(new
        FileInputStream(metaScan)));

    String line = reader.readLine();

    List<Region> regions = new ArrayList<>();

    System.out.print("Processing files.");
    
    while (line != null) {

      if(line.indexOf("column=info:regioninfo")>0){

//        System.out.println(line);

        String tableName = line.split(",")[0];

        TableStats stats = tableStatsMap.get(tableName);

        if(stats==null) {

          stats = new TableStats(tableName);

          tableStatsMap.put(tableName, stats);

        }

        String regionId = line.split("\\.")[1].split("\\.")[0];

        if(regionsSet == null || regionsSet.contains(regionId)){

          String startKey = line.substring(line.indexOf("STARTKEY => "),line
              .indexOf(", ENDKEY"));
          String endKey = line.substring(line.indexOf("ENDKEY => "), line
              .indexOf("}"));
          
          List<String> hFiles = getHFiles(regionId, hdfsList);

          Region region = new Region(regionId, tableName, startKey, endKey,
              hFiles);

          regions.add(region);

          stats.regionCount++;

          long regionSize = region.parseRegionSizeInBytes();

          stats.tableTotalSize += regionSize;

          if( stats.largestRegionSize < regionSize ) {

            stats.largestRegionSize = regionSize;

          } else if (stats.smallestRegionSize > regionSize ) {

            stats.smallestRegionSize = regionSize;

          }

        }

      }

      line = reader.readLine();

    }
    
    System.out.println("");
    reader.close();
    
    return regions;
  }
  
  public static List<String> getHFiles(String regionId, String hdfsList) throws Exception
  {
	  List<String> hFiles = new ArrayList<>();
	  Scanner scanner = new Scanner(new File(hdfsList));
	  String line;
		
	  while (scanner.hasNext()){
		  line = scanner.nextLine();
		  if (line.indexOf("/" + regionId + "/") > 0 && line.indexOf("/hbase/data/") > 0) {
			  if(isHFile(line)) {
				  hFiles.add(line);
				  System.out.print(".");
			  }
		  }
	  }

	  scanner.close();
	
      return hFiles;
  }
  
  public static boolean isHFile(String foldersLine) {
	  if (foldersLine.indexOf("recovered.edits") > 0) return false;
	  if (foldersLine.indexOf(".tmp") > 0) return false;
	  if (foldersLine.indexOf(".regioninfo") > 0) return false;
	  if (foldersLine.indexOf("<dir>") > 0) return false;
	  
	  return true;
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

  static class TableStats {
    String tableName;
    long regionCount;
    long largestRegionSize;
    long smallestRegionSize = Long.MAX_VALUE;
    long tableTotalSize;

    public TableStats(String tableName){

      this.tableName = tableName;

    }

  }

  static class Region implements Comparable<Region>{

    String regionId;

    String startKey;

    String tableName;

    String endKey;
    
    List<String> hFiles;
    
    long regionSizeInBytes;
    
    Integer hFilesCount;

    public Region(String id, String startKey, String endKey){

      this.regionId = id;

      this.startKey = startKey;

      this.endKey = endKey;
      
      this.hFiles = new ArrayList<>();
      
      this.regionSizeInBytes = -1;
      
      this.hFilesCount = -1;
      
    }
    
    public Region(String id, String tableName, String startKey, String endKey,
        List<String> hFiles){

        this.regionId = id;

        this.tableName =tableName;

        this.startKey = startKey;

        this.endKey = endKey;
        
        this.hFiles = hFiles;

        this.regionSizeInBytes = this.parseRegionSizeInBytes();
        
        this.hFilesCount = this.parseHFilesCount();
        
      }
    
    private long parseRegionSizeInBytes() {
    		long sum = 0;
    		
    		for (String line : this.hFiles) {
//    		  System.out.println(line);
    			String sizeString = line.split("\\s+")[4];
    			sum += Long.parseLong(sizeString);
		}
    		
    		return sum;
    }
    
    private Integer parseHFilesCount() {
		return this.hFiles.size();
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
      return this.regionId + ", table: " + this.tableName + ", " + startKey +
          ", " +
          endKey + ", "
          + "hFiles: " + hFilesCount + ", size: " + (new DecimalFormat("#.##")).format((double)(regionSizeInBytes) / (1024 * 1024)) + " MB";
    }
  }



}

