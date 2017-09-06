package com.pinterest.yuvi.chunk;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.pinterest.yuvi.metricandtagstore.MetricAndTagStore;
import com.pinterest.yuvi.metricandtagstore.MetricsAndTagStoreImpl;
import com.pinterest.yuvi.metricstore.MetricStore;
import com.pinterest.yuvi.metricstore.VarBitMetricStore;
import com.pinterest.yuvi.metricstore.offheap.OffHeapVarBitMetricStore;
import com.pinterest.yuvi.models.Point;
import com.pinterest.yuvi.models.Points;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.InvertedIndexTagStore;
import com.pinterest.yuvi.tagstore.Metric;
import com.pinterest.yuvi.tagstore.Query;
import com.pinterest.yuvi.tagstore.TagStore;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Each instance of yuvi stores the last N hours of time series data. That data is broken down
 * into 2 hours worth of time series data called a chunk. So, if the ChunkManager stores a day worth
 * of data there will be 12 chunks in a yuvi instance. The ChunkManager is a class that manages
 * the chunks in a yuvi instance.
 */
@SuppressWarnings("unchecked")
public class ChunkManager {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkManager.class);

  public static Duration DEFAULT_CHUNK_DURATION = Duration.ofMinutes(120);  // 2 hours.

  private Object chunkMapSync = new Object();

  /**
   * Each chunk contains 2 hours worth of data. The chunk map is a map whose key is start time of a
   * 2 hours timestamp and value is a chunk for those 2 hours.
   */
  private final Map<Long, Chunk> chunkMap;

  private final String chunkDataPrefix;

  private final TagStore tagStore;

  public ChunkManager(String chunkDataPrefix, int expectedTagStoreSize) {
    chunkMap = new ConcurrentHashMap<>();
    this.chunkDataPrefix = chunkDataPrefix;
    this.tagStore = new InvertedIndexTagStore(expectedTagStoreSize);
  }

  private Chunk makeChunk(long startTime) {
    Instant endTime = Instant.ofEpochSecond(startTime)
        .plusMillis(DEFAULT_CHUNK_DURATION.toMillis());

    return new ChunkImpl(
        new MetricsAndTagStoreImpl(tagStore, new VarBitMetricStore()),
        new ChunkInfo(chunkDataPrefix + "_" + startTime, startTime, endTime.getEpochSecond()));
  }

  /**
   * Get or create a chunk for a specific timestamp from chunkMap.
   */
  public Chunk getChunk(long timestamp) {
    // Check current chunk range.
    long twoHourTimestampOverage = timestamp % DEFAULT_CHUNK_DURATION.getSeconds();
    long blockHeaderTimestamp = timestamp - twoHourTimestampOverage;
    if (chunkMap.containsKey(blockHeaderTimestamp)) {
      return chunkMap.get(blockHeaderTimestamp);
    } else {
      // Since multiple points may be inserted at the same time and map updated sync here.
      synchronized (chunkMapSync) {
        Chunk newChunk = makeChunk(blockHeaderTimestamp);
        Chunk prevChunk = chunkMap.putIfAbsent(blockHeaderTimestamp, newChunk);
        if (prevChunk == null) {
          return newChunk;
        } else {
          // Because of the synchronized lock only one thread can create a chunk once.
          LOG.error("Possible race condition in Chunk Manager.");
          return prevChunk;
        }
      }
    }
  }

  /**
   * Parse and ingest metric string. The input metric string is in the following format.
   * Sample msg: put tc.proc.net.compressed.jenkins-worker-mp 1465530393 0 iface=eth0 direction=in
   *
   * The logic to parse the opentsdb metric string is added inline here intentionally. We can add
   * a separate parse method, but since Java can't return multiple arguments we have to create an
   * additional object to return these fields for a short time. This method is hot since it is
   * called for every metric added. Instead of adding additional garbage, inlining the logic here
   * for now. This is ok for now since the metrics are only in one format for now. If we need to
   * handle metrics in multiple formats in future, we can make this logic more pluggable.
   */
  public void addMetric(final String metricString) {
    try {
      String[] metricParts = metricString.split(" ");
      if (metricParts.length > 1 && metricParts[0].equals("put")) {
        String metricName = metricParts[1].trim();
        List<String> rawTags = Arrays.asList(metricParts).subList(4, metricParts.length);
        Metric metric = new Metric(metricName, rawTags);
        long ts = Long.parseLong(metricParts[2].trim());
        double value = Double.parseDouble(metricParts[3].trim());

        Chunk chunk = getChunk(ts);
        chunk.addPoint(metric, ts, value);
      } else {
        throw new IllegalArgumentException("Invalid metric string " + metricString);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid metric string " + metricString, e);
    }
  }

  /**
   * Query multiple chunks that contain data between startTs and endTs, merge their results. Return
   * a single list of time series events. This query assumes that the startTs and endTs align with
   * 2 hour boundaries. Otherwise, we return data that aligns at 2 hour boundaries.
   *
   * Currently, the underlying time series store has to decode the entire time series to get the
   * values at the end of the series. So, it's better to return the entire range. Statsboard and
   * other UIs query for data at hour boundary any ways.
   *
   * Optimizations to be considered
   * Query chunks in parallel.
   * Minimize the number of intermediate objects created.
   * Simplify logic using other collectors?
   * Optimize chunk search. Currently, we search through 12 chunk every time.
   * Only query the chunk for time range for the query.
   */
  public List<TimeSeries> queryAroundChunkBoundaries(Query query, long startTsSecs,
                                                     long endTsSecs,
                                                     QueryAggregation queryAggregation) {

    // Select relavent chunks
    List<Chunk> chunksContainingData = chunkMap.values().stream()
        .filter(chunk -> chunk.containsDataInTimeRange(startTsSecs, endTsSecs))
        .collect(toList());

    // Query the chunks.
    List<List<TimeSeries>> pointsFromChunks = chunksContainingData.stream()
        .map(chunk -> chunk.query(query))
        .collect(toList());

    // Group results by metric name
    Map<String, List<List<Point>>> pointsByMetricName = pointsFromChunks.stream()
        .flatMap(List::stream)
        .map(series -> new AbstractMap.SimpleEntry<>(series.getMetric(), series.getPoints()))
        .collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toList())));

    if (queryAggregation.equals(QueryAggregation.ZIMSUM)) {
      // Run zimsum
      Map<Long, Double> pointsZimsum = pointsByMetricName.values().stream()
          .flatMap(List::stream)
          .flatMap(List::stream)
          .collect(Collectors.groupingBy(Point::getTs,
              Collectors.summingDouble(Point::getVal)));

      List<Point> aggregatedPoints = pointsZimsum.entrySet().stream()
          .map(entry -> new Point(entry.getKey(), entry.getValue()))
          .collect(toList());

      return Arrays.asList(
          new TimeSeries(queryAggregation + " " + query.toString(), aggregatedPoints));
    } else {
      // Merge the points into a single timeseries
      return pointsByMetricName.entrySet().stream()
          .map(metricKey -> {
            List<Point> points = metricKey.getValue().stream()
                    .flatMap(List::stream)
                    .collect(toList());
            return new TimeSeries(metricKey.getKey(), Points.dedup(points));
          })
          .collect(toList());
    }
  }

  /*
   * Query multiple chunks that contain data between scartTs and endTs, merge their results. Return
   * a single list of time series events.
   */
  public List<TimeSeries> query(Query query, long startTsSecs, long endTsSecs,
                                QueryAggregation queryAggregation) {
    return queryAroundChunkBoundaries(query, startTsSecs, endTsSecs, queryAggregation);
  }

  @VisibleForTesting
  Map<Long, Chunk> getChunkMap() {
    return chunkMap;
  }

  /**
   * This code is only called during tests and benchmarks. So, the big sync lock is not an issue
   * in practice.
   */
  @VisibleForTesting
  void toOffHeapChunkMap() {
    Map<Long, Chunk> offHeapChunkMap = new ConcurrentHashMap<>();

    synchronized (chunkMapSync) {
      chunkMap.entrySet().stream().forEach(entry -> {
        Chunk offHeapChunk = toOffHeapChunk(entry.getValue());
        offHeapChunkMap.put(entry.getKey(), offHeapChunk);
      });

      chunkMap.clear();
      chunkMap.putAll(offHeapChunkMap);
    }
  }

  private Chunk toOffHeapChunk(Chunk chunk) {
    ChunkImpl chunkImpl = (ChunkImpl) chunk;
    MetricsAndTagStoreImpl metricsAndTagStore = (MetricsAndTagStoreImpl) chunkImpl.getStore();
    Map seriesMap = (metricsAndTagStore).getMetricStore().getSeriesMap();

    MetricStore offHeapMetricStore =
        OffHeapVarBitMetricStore.toOffHeapStore(seriesMap, chunk.info().dataSet);

    MetricAndTagStore newMetricAndTagStore =
        new MetricsAndTagStoreImpl(metricsAndTagStore.getTagStore(), offHeapMetricStore);

    return new ChunkImpl(newMetricAndTagStore, chunkImpl.info());
  }

  public void toReadOnlyChunks(List<Map.Entry<Long, Chunk>> expiredChunks) {
    LOG.info("Chunks past cut off are: " + expiredChunks);

    expiredChunks.forEach(entry -> {
      final Chunk chunk = entry.getValue();
      LOG.info("Moving chunk to off heap: " + chunk.info());

      Chunk readOnlyChunk = toOffHeapChunk(chunk);

      synchronized (chunkMapSync) {
        Chunk oldChunk = chunkMap.put(entry.getKey(), readOnlyChunk);
        // Close the old chunk to free up memory faster.
        oldChunk.close();
      }

      LOG.info("Moved chunk to off heap: " + chunk.info());
    });
  }
}
