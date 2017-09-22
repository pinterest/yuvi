package com.pinterest.yuvi.utils;

import com.pinterest.yuvi.chunk.Chunk;
import com.pinterest.yuvi.tagstore.Metric;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MetricUtils {
  public static void parseAndAddOpenTsdbMetric(String metricString, Chunk chunk) {
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
    return "put " + metricName + " " + Long.toString(ts) + " " + Double.toString(value) + " "
        + tags;
  }

  /**
   * Convert a graphite metric to an OpenTSDB metric
   */
  public static String graphiteToOpenTsdbMetric(String graphiteMetric) {
    String[] words = graphiteMetric.split(" ");

    if (words.length != 3) {
      throw new IllegalArgumentException("Invalid graphite metric: " + graphiteMetric);
    }

    String metricName = words[0];
    Metric m = new Metric(metricName, Collections.emptyList());
    long ts = Long.parseLong(words[2]);
    double value = Double.parseDouble(words[1]);

    return MetricUtils.makeMetricString(metricName, "", ts, value);
  }
}
