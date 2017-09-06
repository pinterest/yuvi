package com.pinterest.yuvi.metricstore;

import com.pinterest.yuvi.models.Point;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Implementation of MetricsStore that stores the time-series in compressed byte-arrays.
 */
public class VarBitMetricStore implements MetricStore {

  private static Logger LOG = LoggerFactory.getLogger(VarBitMetricStore.class);

  private HashMap<Long, VarBitTimeSeries> series;
  private final ReentrantReadWriteLock mu;

  /**
   * Create an empty metric store.
   */
  public VarBitMetricStore() {
    series = new HashMap<>();
    mu = new ReentrantReadWriteLock();
  }

  @Override
  public List<Point> getSeries(long uuid) {
    mu.readLock().lock();
    try {
      VarBitTimeSeries s = series.get(uuid);
      if (s == null) {
        return Collections.emptyList();
      }
      return s.read().getPoints();
    } finally {
      mu.readLock().unlock();
    }
  }

  @Override
  public void addPoint(long uuid, long ts, double val) {
    // Grab read lock for short path.
    VarBitTimeSeries s;
    mu.readLock().lock();
    try {
      s = series.get(uuid);
    } finally {
      mu.readLock().unlock();
    }
    if (s == null) {
      // Retry with write lock if short path failed.
      mu.writeLock().lock();
      try {
        s = series.get(uuid);
        if (s == null) {
          s = new VarBitTimeSeries();
          series.put(uuid, s);
        }
      } finally {
        mu.writeLock().unlock();
      }
    }
    s.append(ts, val);
  }

  private ArrayList<Long> getUuids() throws Exception {
    // Copy the keys so that we don't hold the readLock for too long.
    mu.readLock().lock();
    try {
      return new ArrayList<Long>(series.keySet());
    } finally {
      mu.readLock().unlock();
    }
  }

  @Override
  public Map<String, Object> getStats() {
    Map<String, Object> stats = new HashMap<>();
    stats.put("MetricCount", new Double(series.size()));
    List<Map<String, Double>> tsStats =
        series.values().stream().map(ts -> ts.getStats()).collect(Collectors.toList());

    stats.put("TimeStampSizeDistribution",
        tsStats.stream().map(ts -> ts.get("timestamps_dataLength"))
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting())));

    stats.put("ValueSizeDistribution",
        tsStats.stream().map(ts -> ts.get("values_dataLength"))
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting())));

    stats.put("TimeStampByteSize",
        tsStats.stream().mapToLong(ts -> ts.get("timestamps_dataSize").longValue()).sum());

    stats.put("ValuesByteSize",
        tsStats.stream().mapToLong(ts -> ts.get("values_dataSize").longValue()).sum());

    return stats;
  }

  @Override
  public Map getSeriesMap() {
    return series;
  }

  @Override
  public void close() {
    this.series = null;
  }
}
