package com.cloudera.support.analysis.tools;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wchevreuil on 29/08/2017.
 */
public class CSVChartGenerator extends ApplicationFrame {

  public CSVChartGenerator(String path, String metric, String title, String hostFilter, String metricFilter) throws Exception {
    super(metric);
    JFreeChart chart = ChartFactory.createTimeSeriesChart(metricFilter==null?metric:metricFilter, "Time", metricFilter==null?metric:metricFilter,
        createDataSet(path, hostFilter, metricFilter), true, true, false);

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

  public static void main(String[] args) throws  Exception {

    String hostFilter = null;
    String metricFilter = null;

    if (args.length>3) {
      for (int i=3; i<args.length; i++){
        String[] property = args[i].split("=");
        if(property[0].equals("host_filter")){
          hostFilter = property[1];
          System.out.println("hostFilter: " + hostFilter);
        }else if(property[0].equals("metric_filter")){
          metricFilter = property[1];
        }
      }
    }

    CSVChartGenerator chart = new CSVChartGenerator(args[0], args[1], args[2], hostFilter, metricFilter);

    chart.pack();

    RefineryUtilities.centerFrameOnScreen(chart);

    chart.setVisible(true);

  }

  private static XYDataset createDataSet(String filePath, String hostFilter, String metricFilter) throws Exception {

    Map<String, TimeSeries> seriesMap = new HashMap<>();

    TimeSeriesCollection dataset = new TimeSeriesCollection();

    File f = new File(filePath);

    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

    //skip the 1st line, which just contains headers
    String line = reader.readLine();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    Double highestValue = Double.MIN_VALUE;

    String lineHighestValue = null;

    while((line = reader.readLine()) != null){

      if(metricFilter != null && !line.contains(metricFilter)) {
        continue;
      }

      if(hostFilter != null && !line.contains(hostFilter)) {
        continue;
      }

      String[] fields = line.split(",");

      TimeSeries series = seriesMap.get(fields[0] + "-" + fields[1]);

      if(series == null){

        series = new TimeSeries(fields[0]  + "-" + fields[1]);

        seriesMap.put(fields[0]  + "-" + fields[1], series);

        dataset.addSeries(series);

      }

      Double value = Double.parseDouble(fields[3]);

      System.out.println(value);

      if(value>highestValue){

        highestValue = value;

        lineHighestValue = line;

      }

      series.addOrUpdate(new Second(sdf.parse(fields[2].replaceAll("\"",""))), value);

    }

    System.out.println("Line with highest value: " + lineHighestValue);

    return dataset;

  }

}
