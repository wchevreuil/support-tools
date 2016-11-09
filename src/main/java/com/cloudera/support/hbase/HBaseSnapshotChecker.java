package com.cloudera.support.hbase;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class HBaseSnapshotChecker {

  private static List<String> references = new ArrayList<String>();
  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

    try {

      String fileName = args[0];
      File file = new File(fileName);
      FileInputStream fis = new FileInputStream(file);
      BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
      String line = reader.readLine();
      line = reader.readLine();
      while (line != null) {
        String[] elements = line.split("archive");
        if (elements.length >= 2) references.add(elements[1]);// System.out.println(elements[1]);
        // if (elements[1].split("\\w\\.\\w").length > 1) System.out.println(elements[1]);
        line = reader.readLine();
      }
      fis.close();
      reader.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    extractSnapshotReferences(args[0]);

  }

  private static void extractSnapshotReferences(String fileName) {
    try {

      File file = new File(fileName);
      FileInputStream fis = new FileInputStream(file);
      BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
      String line = reader.readLine();
      line = reader.readLine();
      while (line != null) {
        String[] elements = line.split("hbase-snapshot");
        if (elements.length < 2) break;
        // System.out.println(elements[1]);
        if (elements[1].split("\\w\\.\\w").length > 1) {
          System.out.println(elements[1]);
        }
        line = reader.readLine();
      }
      fis.close();
      reader.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
