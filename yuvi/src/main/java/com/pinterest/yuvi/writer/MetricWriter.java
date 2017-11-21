package com.pinterest.yuvi.writer;

/**
 * MetricWriter interface implements the interfaces for ingesting metrics into Yuvi.
 */
public interface MetricWriter {

  void start();

  void close();
}
