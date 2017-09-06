package com.pinterest.yuvi.chunk;

import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.Metric;
import com.pinterest.yuvi.tagstore.Query;

import java.util.List;
import java.util.Map;

/**
 * A chunk stores time series data for a specific time range. It can concurrently store metrics and
 * respond to queries. Optionally a chunk can be read only at which point it can only be queried.
 */
public interface Chunk {

  /**
   * Given a id return a list of points contained for that id.
   * @param query a Metric query.
   * @return a list of points.
   */
  List<TimeSeries> query(Query query);

  /**
   * add a point to an existing time-series, or create a new time-series with the given metric.
   * @param metric a metric object.
   * @param ts unix timestamp in seconds.
   * @param value value for the point.
   */
  void addPoint(Metric metric, long ts, double value);

  /**
   * A chunk contains some metadata like the list of chunks it can contain.
   */
  ChunkInfo info();

  /**
   * A chunk will be available for writes initially. But once we no longer need to write any data to
   * it can be turned into a read only chunk.
   */
  boolean isReadOnly();

  /**
   * Return true if the chunk contains data within that time range.
   */
  boolean containsDataInTimeRange(long startTs, long endTs);

  Map<String, Object> getStats();

  /**
   * Close the chunk.
   */
  void close();
}
