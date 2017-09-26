package com.pinterest.yuvi.writer.kafka;

import com.pinterest.yuvi.writer.MetricWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsWriterTask implements Runnable {

  private final MetricWriter metricWriter;

  private static Logger LOG = LoggerFactory.getLogger(MetricsWriterTask.class);

  public MetricsWriterTask(MetricWriter metricWriter) {
    this.metricWriter = metricWriter;
  }

  @Override
  public void run() {
    LOG.info("Starting metrics reader task");

    // Cleanly shutdown the metrics reader upon exit.
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        metricWriter.close();
      }
    });

    try {
      metricWriter.start();
    } catch (Exception e) {
      LOG.error("Error processing metrics", e);
    }
  }
}
