package com.cloudera.support.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by wchevreuil on 11/05/2018.
 */
public class HBaseClientRequestsAnalyser {

  private static final String BUSY_MESSAGE = "RegionTooBusyException";

  private static final String SUCCESS_REGEX = ".*AsyncProcess.*attempt=\\d*/35.*succeeded.*";

  public static void main (String[] args) throws  Exception {

    Map<String,RequestHistory> requests = new HashMap<>();

    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[0]))));

    String line = reader.readLine();

    while(line!=null){

      if(line.indexOf(BUSY_MESSAGE)>=0){

        String asyncProcess = line.split("AsyncProcess: ")[1].split(",")[0];

        RequestHistory requestHistory = requests.get(asyncProcess);

        if(requestHistory==null){

          requestHistory = new RequestHistory();

          requestHistory.failedAttempts = 1;

          requests.put(asyncProcess,requestHistory);

        } else {

          requestHistory.failedAttempts++;

        }

      } else if(Pattern.matches(SUCCESS_REGEX, line)){

        String asyncProcess = line.split("AsyncProcess: ")[1].split(",")[0];

        RequestHistory requestHistory = requests.get(asyncProcess);

        if(requestHistory!=null){

          requestHistory.status = true;

          String[] lineSplit = line.split("INFO");

          String successTimestamp = lineSplit[0];

          String successAttempt = lineSplit[1].split("attempt=")[1].split(" ")[0];

          requestHistory.successfulAttempt = successTimestamp + " -> " + successAttempt;

        }

      }

      line = reader.readLine();

    }

    reader.close();

    for(String asyncProcess : requests.keySet()) {

      RequestHistory requestHistory = requests.get(asyncProcess);

      StringBuilder builder = new StringBuilder();

      builder.append("AsyncProcess: ")
          .append(asyncProcess)
          .append(", failedAttempts: ")
          .append(requestHistory.failedAttempts)
          .append(", succeeded later? ")
          .append(requestHistory.status);

      if(requestHistory.status){
        builder.append(", succeeded at: ")
            .append(requestHistory.successfulAttempt);
      }

      System.out.println(builder.toString());

    }

  }

  public static class RequestHistory {

    int failedAttempts;

    boolean status;

    String successfulAttempt;

  }
}
