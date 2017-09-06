package com.pinterest.yuvi.tagstore;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * In time series a single measurement looks as follows:
 * cpu.util       host=h1 dc=dc1 1234567   10
 * metric_name    tag1    tag2   timestamp  value
 *
 * A metric is a unique combination of metric_name and the tags. So,
 * cpu.util host=h1 dc=dc1
 * cpu.util host=h2 dc=dc1
 * would be considered 2 different metrics.
 *
 * In a TSDB, instead of passing the tags around, we would like to refer to them by an integer.
 * Further, we would also like to query for metrics by saying get me all metrics with the name
 * cpu.util and we should return both the metrics shown above.
 *
 * A tag store is a storage engine that allows storing and retrieving these tags. So, the API maps
 * a metric name and a tag to a UUID and vice versa.
 */
public interface TagStore {

  /**
   * For a given metric return an integer id.
   * @param metric
   * @return
   */
  public Optional<Integer> get(Metric metric);

  /**
   * Return an id for the metric if it exists or create a new one and return that.
   * @param metric
   * @return
   */
  public int getOrCreate(Metric metric);

  /**
   * Lookup the metric idss that match a given metric query.
   * @param metricQuery
   * @return
   */
  public List<Integer> lookup(Query metricQuery);

  /**
   * Return the name given an ID.
   * @param uuid
   * @return
   */
  String getMetricName(int uuid);

  Map<String, Object> getStats();

  void close();
}
