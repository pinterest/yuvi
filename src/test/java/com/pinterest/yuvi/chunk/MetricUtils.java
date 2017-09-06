package com.pinterest.yuvi.chunk;

import com.pinterest.yuvi.tagstore.Metric;

import java.util.Arrays;
import java.util.List;

public class MetricUtils {
  public static void parseAndAddOpenTSDBMetric(String metricString, Chunk chunk) {
    try {
      String[] metricParts = metricString.split(" ");
      if (metricParts.length > 1 && metricParts[0].equals("put")) {
        String metricName = metricParts[1].trim();
        List<String> rawTags = Arrays.asList(metricParts).subList(4, metricParts.length);
        Metric metric = new Metric(metricName, rawTags);
        long ts = Long.parseLong(metricParts[2].trim());
        double value = Double.parseDouble(metricParts[3].trim());

        chunk.addPoint(metric, ts, value);
      } else {
        throw new IllegalArgumentException("Invalid metric string " + metricString);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid metric string " + metricString, e);
    }
  }

  public static String makeMetricString(String metricName, String tags, long ts, double value) {
    // Sample msg: put tc.proc.net.compressed.jenkins-worker-mp 1465530393 0 iface=eth0 direction=in
    return "put " + metricName + " " + Long.toString(ts) + " " + Double.toString(value) + " " + tags;
  }
}
