package com.cloudera.support.gcanalysis;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class GCLogAnalyser {

  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub

    (new GCLogAnalyser()).printYoungGCLongTime(args[0]);

  }

  private void printYoungGCLongTime(String file) throws IOException {

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(new File(file))));

    String line = reader.readLine();

    while (line != null) {

      if (line.startsWith(" [PSYoungGen")) {

        String time = null;

        try {
          time = line.split(" ")[9].split("=")[1];

          if (Double.parseDouble(time) > 1.0d) {
            System.out.println(line);
          }
        } catch (Exception e) {
          System.out.println(line);
          System.out.println(time);
        }
      }

      line = reader.readLine();

    }

    reader.close();

  }
}
