package com.cloudera.support.analysis.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class IOStatAnalyser extends ApplicationFrame {

  public IOStatAnalyser(String path, String metric, String[] disks) throws Exception {
    super(metric);
    JFreeChart chart = ChartFactory.createTimeSeriesChart(path, "Seconds", metric,
      createDataset(path, metric, disks), false, false, false);

    ChartPanel chartPanel = new ChartPanel(chart);
    // chartPanel.setPreferredSize(new java.awt.Dimension(560, 367));

    XYPlot plot = (XYPlot) chart.getPlot();
    XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
    // XYItemRenderer renderer = plot.getRenderer();
    // // renderer.setSeriesPaint(0, Color.blue);
    // NumberAxis domain = (NumberAxis) plot.getDomainAxis();
    // domain.setRange(1485974751000L, 1485974999000L);
    // domain.setTickUnit(new NumberTickUnit(2000));
    // domain.setVerticalTickLabels(true);
    // NumberAxis range = (NumberAxis) plot.getRangeAxis();
    // range.setRange(0, 10000);
    // range.setTickUnit(new NumberTickUnit(1));
    // range.setVerticalTickLabels(true);
    // renderer.setSeriesPaint(0, Color.RED);
    // // renderer.setSeriesPaint(1, Color.GREEN);
    // // renderer.setSeriesPaint(2, Color.YELLOW);
    // // renderer.setSeriesStroke(0, new BasicStroke(4.0f));
    // // renderer.setSeriesStroke(1, new BasicStroke(3.0f));
    // // renderer.setSeriesStroke(2, new BasicStroke(2.0f));
    plot.setRenderer(renderer);
    setContentPane(chartPanel);
  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    String[] disks = { "sda", "sdb", "sdc", "sdd", "sde", "sdf", "sdg", "sdh", "sdi", "sdj", "sdk",
        "sdl", "sdm", "sdn", "sdo", "sdp" };

    IOStatAnalyser chart = new IOStatAnalyser(
        "/Users/wchevreuil/Downloads/iostat/iostat_dlsdnr10n66.globetel.com.txt", "w_await", disks);

    chart.pack();

    RefineryUtilities.centerFrameOnScreen(chart);

    chart.setVisible(true);

    // for (File f : (new File("/Users/wchevreuil/Downloads/iostat/")).listFiles()) {
    // System.out.println("-------> " + f.getName());
    // createDataset(f.getAbsolutePath(), "w_await", disks);
    // }

  }

  private static XYDataset createDataset(String path, String metric, String[] disks)
      throws Exception {

    Map<String, TimeSeries> seriesMap = new HashMap<>();

    TimeSeriesCollection dataset = new TimeSeriesCollection();

    for (String disk : disks) {

      TimeSeries series = new TimeSeries(disk);

      seriesMap.put(disk, series);

      dataset.addSeries(series);

    }

    int metricIdx = -1;

    File f = new File(path);

    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

    String line = null;

    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");

    Date event = null;

    Second second = null;

    while ((line = reader.readLine()) != null) {

      try {

        event = sdf.parse(line);

        second = new Second(event);

      } catch (ParseException e) {

        if (!line.matches("\\s*")) {

          String[] columns = line.split("\\s+");

          if (columns.length == 14) {

            if (!line.matches("Device.*")) {

              seriesMap.get(columns[0]).add(second,
                Double.parseDouble(columns[metricIdx]));

              if (Double.parseDouble(columns[metricIdx]) > 300) {
                System.out.println("added serie for " + columns[0] + ": " + columns[metricIdx] + ","
                    + event);
              }

            } else {

              if (metricIdx < 0) {

                for (int i = 1; i < columns.length; i++) {

                  if (columns[i].equals(metric)) {

                    metricIdx = i;

                  }
                }
              }

            }

          }

        }

      }
    }

    reader.close();

    return dataset;
  }

}
