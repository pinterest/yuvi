package com.pinterest.yuvi.chunk;

import com.pinterest.yuvi.metricandtagstore.MetricAndTagStore;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.Metric;
import com.pinterest.yuvi.tagstore.Query;

import java.util.List;
import java.util.Map;

public class ChunkImpl implements Chunk {

  private final MetricAndTagStore store;
  private final ChunkInfo chunkInfo;

  public ChunkImpl(MetricAndTagStore store, ChunkInfo chunkInfo) {
    this.store = store;
    this.chunkInfo = chunkInfo;
  }

  @Override
  public List<TimeSeries> query(Query query) {
    return store.getSeries(query);
  }

  @Override
  public void addPoint(Metric metric, long ts, double value) {
    store.addPoint(metric, ts, value);
  }

  @Override
  public ChunkInfo info() {
    return chunkInfo;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public boolean containsDataInTimeRange(long startTs, long endTs) {
    return (chunkInfo.startTimeSecs <= startTs && chunkInfo.endTimeSecs >= startTs)
        || (chunkInfo.startTimeSecs <= endTs && chunkInfo.endTimeSecs >= endTs)
        || (chunkInfo.startTimeSecs >= startTs && chunkInfo.endTimeSecs <= endTs);
  }

  @Override
  public Map<String, Object> getStats() {
    return store.getStats();
  }

  @Override
  public void close() {
    store.close();
  }

  public MetricAndTagStore getStore() {
    return store;
  }

  @Override
  public String toString() {
    return "ChunkImpl{"
        + "chunkInfo=" + chunkInfo
        + " ,store=" + store
        + '}';
  }
}
