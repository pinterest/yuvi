package com.pinterest.yuvi.metricandtagstore;

import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.Metric;
import com.pinterest.yuvi.tagstore.Query;

import java.util.List;
import java.util.Map;

/**
 * This interface provides a higher level interface over the metric and tag store. The interface
 * let's users add a metric and a value and similarly retrieve all the points for a given
 * and metric.
 */
public interface MetricAndTagStore {

  /**
   * Given a id return a list of points contained for that id.
   * @param query a Metric query.
   * @return a list of points.
   */
  List<TimeSeries> getSeries(Query query);

  /**
   * add a point to an existing time-series, or create a new time-series with the given uuid.
   * @param metric a metric object.
   * @param ts unix timestamp in seconds.
   * @param val value for the point.
   */
  void addPoint(Metric metric, long ts, double val);

  Map<String, Object> getStats();

  /**
   * Close the metric and tag store cleanly.
   */
  void close();

  /**
   * Returns true if the metric or tag store is marked as read only.
   */
  boolean isReadOnly();
}
