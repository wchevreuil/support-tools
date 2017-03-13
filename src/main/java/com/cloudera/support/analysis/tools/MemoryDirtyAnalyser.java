package com.cloudera.support.analysis.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class MemoryDirtyAnalyser extends ApplicationFrame {

  public MemoryDirtyAnalyser(String title, String path) throws Exception {
    super(title);

    JFreeChart chart = ChartFactory.createTimeSeriesChart("Memory Dirty over Time", "DateTime",
      "TotalDirty (KB)",
      createDataset(path,
        /* (new SimpleDateFormat("dd/MM/yyyy hh:mm:ss")).parse("25/02/2017 00:00:00") */null),
      false, true, true);

    ChartPanel chartPanel = new ChartPanel(chart);

    chartPanel.setPreferredSize(new java.awt.Dimension(1460, 867));

    XYPlot plot = (XYPlot) chart.getPlot();

    DateAxis xAxis = new DateAxis();

    xAxis.setDateFormatOverride(new SimpleDateFormat("dd/MM/YYYY hh:mm:ss z"));

    plot.setDomainAxis(xAxis);

    XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();

    plot.setRenderer(renderer);

    setContentPane(chartPanel);

  }

  public static void main(String[] args) throws Exception {

    MemoryDirtyAnalyser chart =
        new MemoryDirtyAnalyser("Memory Dirty over Time",
            "/Users/wchevreuil/Downloads/123999/dirty");

    chart.pack();

    RefineryUtilities.centerFrameOnScreen(chart);

    chart.setVisible(true);

  }

  private static XYDataset createDataset(String path, Date endDate) throws Exception {

    TimeSeriesCollection dataset = new TimeSeriesCollection();

    File dir = new File(path);

    File output = new File("/Users/wchevreuil/Downloads/123999/result.csv");

    BufferedWriter writer =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output)));

    for (File f : dir.listFiles()) {

      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

      String line = null;

      SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd hh:mm:ss z yyyy");

      Date event = null;

      Second second = null;

      TimeSeries series = new TimeSeries(f.getName());

      dataset.addSeries(series);

      int dirty = 0;

      int writeBack = 0;

      int writeBackTmp = 0;

      while ((line = reader.readLine()) != null) {

        try {

          event = sdf.parse(line);

          if (endDate != null && event.after(endDate)) {

            break;

          }

          second = new Second(event);

          // System.out.println(line);

        } catch (ParseException e) {

          String[] lineSplit = line.split("\\s+");

          switch (lineSplit[0]) {
          case "Dirty:":
            dirty = Integer.parseInt(lineSplit[1]);
            break;
          case "Writeback:":
            writeBack = Integer.parseInt(lineSplit[1]);
            break;
          case "WritebackTmp:":
            writeBackTmp = Integer.parseInt(lineSplit[1]);
            series.addOrUpdate(second, (dirty + writeBack + writeBackTmp));
            if ((dirty + writeBack + writeBackTmp) >= 200000) {
              System.out.println(
                event + " ==> " + (dirty + writeBack + writeBackTmp) + " on " + f.getName());
              writer.write(event + "," + (dirty + writeBack + writeBackTmp) + "," + dirty + ","
                  + writeBack + "," + writeBackTmp + "," + f.getName());
              writer.newLine();
            }
            dirty = 0;
            writeBack = 0;
            writeBackTmp = 0;
            break;

          }
        }

      }

      reader.close();
    }

    writer.close();

    return dataset;

  }

}
