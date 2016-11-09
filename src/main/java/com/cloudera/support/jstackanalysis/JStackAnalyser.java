package com.cloudera.support.jstackanalysis;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JStackAnalyser {


  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    HashMap<String, List<String>> map = (new JStackAnalyser()).readJstakFiles(args[0]);

    List<TheadEntry> entries = new ArrayList<TheadEntry>();

    for (String key : map.keySet()) {
      entries.add(new TheadEntry(key, map.get(key).size()));
    }

    Collections.sort(entries);

    for (TheadEntry entry : entries) {
      System.out.println(entry);
    }

    // optionally, write details for a specific threadId, for instance
    // printSummaryForThread("2054637940@qtp-829766234-62",map);
    if (args.length == 2) {
      printSummaryForThread(args[1], map);
    }

  }

  private static void printSummaryForThread(String threadId, Map<String, List<String>> threadsMap) {

    System.out
        .println("=============== Reading " + threadId + " entries ==================");

    HashMap<String, Integer> stacks = new HashMap<String, Integer>();

    for (String trace : threadsMap.get(threadId)) {
      if (stacks.get(trace) == null) {
        stacks.put(trace, 1);
      } else {
        stacks.put(trace, stacks.get(trace) + 1);
      }
    }

    for (String stack : stacks.keySet()) {
      System.out.println(stacks.get(stack) + " ----> " + stack);
    }

  }

  private HashMap<String, List<String>> readJstakFiles(String file) throws IOException {

    File dir = new File(file);

    HashMap<String, List<String>> map = new HashMap<String, List<String>>();

    for (File f : dir.listFiles()) {
      System.out.println(f);
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

      String line = reader.readLine();

      String key = null;

      StringBuilder jstack = null;

      while (line != null) {

        if (line.startsWith("\"")) {

          if (key != null) {

            if (map.get(key) == null) {
              map.put(key, new ArrayList<String>());
            }

          }

          if (jstack != null) {

            map.get(key).add(jstack.toString());

          }

          key = line.split("\"")[1];

          jstack = new StringBuilder(line);

        }

        if (jstack != null) {
          jstack.append(line);
        }

        line = reader.readLine();

      }

      if (map.get(key) == null) {
        map.put(key, new ArrayList<String>());
      }

      map.get(key).add(jstack.toString());

      reader.close();

    }

    return map;
  }

}

class TheadEntry implements Comparable<TheadEntry> {

  String name;
  Integer count;

  public TheadEntry(String name, Integer count) {
    this.name = name;
    this.count = count;
  }

  @Override
  public int compareTo(TheadEntry o) {
    if (this.count == o.count) {
      return 0;
    } else {
      return this.count > o.count ? 1 : -1;
    }
  }

  @Override
  public String toString() {
    return name + ": " + count;
  }

}
