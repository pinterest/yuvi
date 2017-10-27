package com.pinterest.yuvi.metricstore;

import com.pinterest.yuvi.models.Point;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Implementation of MetricsStore that stores the time-series in compressed byte-arrays.
 */
public class VarBitMetricStore implements MetricStore {

  private static Logger LOG = LoggerFactory.getLogger(VarBitMetricStore.class);

  // TODO: Tune the default metrics size
  private static final int DEFAULT_METRIC_STORE_SIZE = 10_000;

  private Map<Long, VarBitTimeSeries> series;

  public VarBitMetricStore() {
    this(DEFAULT_METRIC_STORE_SIZE);
  }

  /**
   * Create an empty metric store.
   */
  public VarBitMetricStore(int initialSize) {
    series = new ConcurrentHashMap<>(initialSize);

    LOG.info("Created a var bit metric store with size {}.", initialSize);
  }

  @Override
  public List<Point> getSeries(long uuid) {
    VarBitTimeSeries s = series.get(uuid);
    if (s == null) {
      return Collections.emptyList();
    }
    return s.read().getPoints();
  }

  @Override
  public void addPoint(long uuid, long ts, double val) {
    VarBitTimeSeries s = series.computeIfAbsent(uuid, k -> new VarBitTimeSeries());
    s.append(ts, val);
  }

  private List<Long> getUuids() {
    return new ArrayList<>(series.keySet());
  }

  @Override
  public Map<String, Object> getStats() {
    Map<String, Object> stats = new HashMap<>();
    stats.put("MetricCount", (double) series.size());
    List<Map<String, Double>> tsStats =
        series.values().stream().map(VarBitTimeSeries::getStats).collect(Collectors.toList());

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

  @Override
  public boolean isReadOnly() {
    return false;
  }
}
