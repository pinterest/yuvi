package com.pinterest.yuvi.metricstore.offheap;

import com.pinterest.yuvi.metricstore.MetricStore;
import com.pinterest.yuvi.metricstore.TimeSeriesIterator;
import com.pinterest.yuvi.metricstore.VarBitTimeSeries;
import com.pinterest.yuvi.models.Point;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The off heap metric store stores a snapshot of the metrics off heap.
 */
public class OffHeapVarBitMetricStore implements MetricStore {

  private static Logger LOG = LoggerFactory.getLogger(OffHeapVarBitMetricStore.class);

  private static final int DEFAULT_VALUE_SIZE = 2000;

  private Map<LongValue, ByteBuffer> timeSeries;

  private static final String offHeapNamePrefix = "yuvi_timeseries";

  private final String chunkInfo;

  public OffHeapVarBitMetricStore(long size, String chunkInfo) {
    this(size, DEFAULT_VALUE_SIZE, chunkInfo);
  }

  public OffHeapVarBitMetricStore(long size, int valueSize, String chunkInfo) {
    this(size, valueSize, chunkInfo, "");
  }

  public OffHeapVarBitMetricStore(long size, int valueSize, String chunkInfo, String dir) {
    this.chunkInfo = chunkInfo;

    ChronicleMapBuilder<LongValue, ByteBuffer> mapBuilder = ChronicleMap
        .of(LongValue.class, ByteBuffer.class)
        .entries(size)
        .averageValueSize(valueSize);

    if (chunkInfo != null && !chunkInfo.isEmpty() && !dir.isEmpty()) {
      File offHeapFile = new File(dir + "/" + offHeapNamePrefix + "_" + chunkInfo);
      try {
        timeSeries = mapBuilder.name(offHeapNamePrefix + "_" + chunkInfo)
            .createPersistedTo(offHeapFile);
      } catch (IOException e) {
        LOG.error("Failed to create an offheap store {} with error {}", offHeapFile, e.getMessage());
        throw new IllegalArgumentException("Failed to create an off heap store.", e);
      }
    } else {
      timeSeries = mapBuilder.name(offHeapNamePrefix).create();
    }
    LOG.info("Created an off heap metric store of size={} valueSize={} chunkInfo={} in dir={}",
        size, valueSize, chunkInfo, dir);
  }

  @Override
  public List<Point> getSeries(long uuid) {
    final LongValue key = Values.newHeapInstance(LongValue.class);
    key.setValue(uuid);
    if (timeSeries.containsKey(key)) {
      ByteBuffer serializedValues = timeSeries.get(key);
      TimeSeriesIterator iterator = VarBitTimeSeries.deserialize(serializedValues);
      return iterator.getPoints();
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Create an OffHeapMetricStore from a MetricStore.
   * TODO: We use the max value size for all values. But it can be tuned.
   */
  public static OffHeapVarBitMetricStore toOffHeapStore(Map<Long, VarBitTimeSeries> timeSeriesMap,
                                                        String chunkInfo, String dataDirectory) {

    int maxSize = timeSeriesMap.values().stream()
        .mapToInt(series -> series.getSerializedByteSize())
        .max()
        .getAsInt();

    OffHeapVarBitMetricStore offHeapStore =
        new OffHeapVarBitMetricStore(timeSeriesMap.size(), maxSize, chunkInfo, dataDirectory);

    timeSeriesMap.entrySet().forEach(e -> {
      try {
        VarBitTimeSeries series = e.getValue();

        int serializedByteSize = series.getSerializedByteSize();
        ByteBuffer serializedTimeSeriesBuffer = ByteBuffer.allocate(serializedByteSize);
        series.serialize(serializedTimeSeriesBuffer);
        // This is needed because JVM is big-endian but linux native memory is little-endian.
        serializedTimeSeriesBuffer.flip();
        offHeapStore.addPoint(e.getKey(), serializedTimeSeriesBuffer);
      } catch (Exception ex) {
        LOG.info("Moving entry {} in chunk {} to off heap failed with exception {}",
            e.getKey(), chunkInfo, ex);
      }
    });
    return offHeapStore;
  }

  public void addPoint(long uuid, ByteBuffer series) {
    LongValue key = Values.newHeapInstance(LongValue.class);
    key.setValue(uuid);
    timeSeries.put(key, series);
  }

  @Override
  public void addPoint(long uuid, long ts, double val) {
    throw new UnsupportedOperationException("This is a read only metric store");
  }

  @Override
  public Map<String, Object> getStats() {
    Map<String, Object> stats = new HashMap<>();
    stats.put("MetricCount", new Double(timeSeries.size()));
    stats.put("TimeSeriesByteSize",
        timeSeries.values().stream().mapToInt(ts -> ts.capacity()).sum());
    return stats;
  }

  @Override
  public Map getSeriesMap() {
    return timeSeries;
  }

  @Override
  public void close() {
    ChronicleMap timeSeries = (ChronicleMap) this.timeSeries;
    LOG.info("Closing chronicle map {}", chunkInfo);
    // Closing the timeSeries frees up all the resources associated with this map if there are no
    // other references to it. So, make sure all the references to the map are closed.
    timeSeries.close();
    LOG.info("Closed chronicle map {}.", chunkInfo);
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public void setReadOnly(boolean readOnly) {
    throw new UnsupportedOperationException("The store is read-only " + chunkInfo);
  }
}
