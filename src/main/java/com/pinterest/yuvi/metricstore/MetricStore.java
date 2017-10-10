package com.pinterest.yuvi.metricstore;

import com.pinterest.yuvi.models.Point;

import java.util.List;
import java.util.Map;

/**
 * An interface for storing and retrieving individual time-series in-memory. Each time-series is
 * identified by a uuid. Also has the ability to serialize itself to an index. This class is
 * thread-safe.
 */
public interface MetricStore {

  /**
   * Given a id return a list of points contained for that id.
   * @param uuid
   * @return a list of points.
   */
  List<Point> getSeries(long uuid);


  /**
   * add a point to an existing time-series, or create a new time-series with the given uuid.
   * @param uuid the identifier for the series.
   * @param ts unix timestamp in seconds.
   * @param val value for the point.
   */
  void addPoint(long uuid, long ts, double val);

  Map<String, Object> getStats();

  Map getSeriesMap();

  void close();

  boolean isReadOnly();
}
