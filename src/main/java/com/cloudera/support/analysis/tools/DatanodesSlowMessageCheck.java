package com.cloudera.support.analysis.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;

import jline.internal.InputStreamReader;

public class DatanodesSlowMessageCheck {

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    File rootPath = new File(args[0]);

    for (File f : rootPath.listFiles()) {

      System.out.println("printing stats for DN: " + f.getName());

      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

      String line = null;

      long largestDiskCost = 0;

      long largestMirrorCost = 0;

      long largestFlush = 0;

      long largestManageWriter = 0;

      long countDiskCost = 0;

      long countMirrorCost = 0;

      long countFlush = 0;

      long countManageWriter = 0;

      String whenlargestDiskCost = null;

      String whenLargestMirrorCost = null;

      String whenLargestFlush = null;

      String whenLargestManageWriter = null;

      while ((line = reader.readLine()) != null) {

        if (line.indexOf("manageWriterOsCache") > 0) {

          long timeTaken = Long.parseLong(line.split("ms")[0].split("took ")[1]);

          countManageWriter++;

          if (largestManageWriter < timeTaken) {

            largestManageWriter = timeTaken;

            whenLargestManageWriter = line;

          }

        } else if (line.indexOf("write packet") > 0) {

          long timeTaken = Long.parseLong(line.split("ms")[0].split("took ")[1]);

          countMirrorCost++;

          if (largestMirrorCost < timeTaken) {

            largestMirrorCost = timeTaken;

            whenLargestMirrorCost = line;

          }

        } else if (line.indexOf("write data") > 0) {

          long timeTaken = Long.parseLong(line.split("ms")[0].split("cost:")[1]);

          countDiskCost++;

          if (largestDiskCost < timeTaken) {

            largestDiskCost = timeTaken;

            whenlargestDiskCost = line;
          }

        } else if (line.indexOf("flushOrSync") > 0) {

          long timeTaken = Long.parseLong(line.split("ms")[0].split("took ")[1]);

          countFlush++;

          if (largestFlush < timeTaken) {

            largestFlush = timeTaken;

            whenLargestFlush = line;

          }

        }

      }

      System.out
          .println("The longest \"Slow BlockReceiver write data to disk\": " + largestDiskCost);
      System.out.println("The longest \"Slow manageWriterOsCache\": " + largestManageWriter);
      System.out.println(
        "The longest \"Slow BlockReceiver write packet to mirror\": " + largestMirrorCost);
      System.out.println("The longest \"Slow flushOrSync\": " + largestFlush);

      System.out.println(
        "Total of \"Slow BlockReceiver write data to disk\" occurrences: " + countDiskCost);
//      System.out.println(whenlargestDiskCost);


      System.out
          .println("Total of \"Slow manageWriterOsCache\" occurrences: " + countManageWriter);
//      System.out.println(whenLargestManageWriter);


      System.out.println(
        "Total of \"Slow BlockReceiver write packet to mirror\" occurrences: " + countMirrorCost);
//      System.out.println(whenLargestMirrorCost);

      System.out.println("Total of \"Slow flushOrSync\" occurrences: " + countFlush);
//      System.out.println(whenLargestFlush);

      long[] allTimes = { largestDiskCost, largestFlush, largestManageWriter, largestMirrorCost };

      long largest = 0;

      String largestOfAllTimes = null;

      String[] dateTimeOfLargest = null;

      for (int i = 0; i < 4; i++) {

        if (allTimes[i] > largest) {
          largest = allTimes[i];
          switch (i) {
          case 0:
            largestOfAllTimes = "Write to Disk took longest: " + largest;
            dateTimeOfLargest = whenlargestDiskCost.split(" ");
            break;
          case 1:
            largestOfAllTimes = "Flush took longest: " + largest;
            dateTimeOfLargest = whenLargestFlush.split(" ");
            break;
          case 2:
            largestOfAllTimes = "ManageWriter took longest: " + largest;
            dateTimeOfLargest = whenLargestManageWriter.split(" ");
            break;
          case 3:
            largestOfAllTimes = "Write Packet took longest: " + largest;
            dateTimeOfLargest = whenLargestMirrorCost.split(" ");
          }
        }

      }


      System.out.println("What took longer? " + largestOfAllTimes + "; Which time? "
          + dateTimeOfLargest[0] + " " + dateTimeOfLargest[1]);

      System.out.println("-------");

      reader.close();

    }

  }

}
