package com.pinterest.yuvi.metricandtagstore;

import com.pinterest.yuvi.metricstore.MetricStore;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.Metric;
import com.pinterest.yuvi.tagstore.Query;
import com.pinterest.yuvi.tagstore.TagStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * MetricAndTagStore stores the metrics in gorilla format and the tags in an inverted index.
 *
 * TODO: Batch insert API
 * TODO: synchronization?
 * TODO: error handling
 */
public class MetricsAndTagStoreImpl implements MetricAndTagStore {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsAndTagStoreImpl.class);
  private final TagStore tagStore;
  private final MetricStore metricStore;

  public MetricsAndTagStoreImpl(TagStore tagStore, MetricStore metricStore) {
    this.tagStore = tagStore;
    this.metricStore = metricStore;
    LOG.info("Created a new metric store {} and tag store {}.", metricStore, tagStore);
  }

  @Override
  public List<TimeSeries> getSeries(Query query) {
    List<Integer> ids = tagStore.lookup(query);
    // Get an iterator instead?
    // Catch exceptions and make it easy to debug.
    return ids.stream()
        .map(id -> new TimeSeries(tagStore.getMetricName(id), metricStore.getSeries(id)))
        .collect(Collectors.toList());
  }

  public MetricStore getMetricStore() {
    return metricStore;
  }

  public TagStore getTagStore() {
    return tagStore;
  }

  @Override
  public void addPoint(Metric metric, long ts, double val) {
    int metricId = tagStore.getOrCreate(metric);
    metricStore.addPoint(metricId, ts, val);

  }

  @Override
  public Map<String, Object> getStats() {
    Map<String, Object> stats = new HashMap<>();
    metricStore.getStats().forEach((key, value) -> stats.put("metricStore_" + key, value));
    tagStore.getStats().forEach((key, value) -> stats.put("tagStore_" + key, value));
    return stats;
  }

  /**
   * Currently, tag store is shared among chunks. So only close the metric store.
   */
  @Override
  public void close() {
    metricStore.close();
  }

  @Override
  public boolean isReadOnly() {
    return tagStore.isReadOnly() || metricStore.isReadOnly();
  }
}
