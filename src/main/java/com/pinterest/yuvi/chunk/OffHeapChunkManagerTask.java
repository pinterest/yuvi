package com.pinterest.yuvi.chunk;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Yuvi stores all the metrics data in memory, but there is a GC overhead to storing all the metrics
 * data in memory, so we move the metrics data that will be no longer updated to off heap store and
 * mark it read only to save GC overhead. Further, after stale data delay, the data is deleted from
 * the chunk manager.
 *
 * This class identifies chunks in chunk manager that can be moved off heap. A chunk can be moved
 * off heap if the current time is greater than the chunk end time. However, sometimes metrics may
 * be delayed because of transient issues. To ingest late arriving metrics, we wait a metricsDelay
 * amount of time before we start marking the chunk read-only and moving it to off heap.
 */
public class OffHeapChunkManagerTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(OffHeapChunkManagerTask.class);

  static final int DEFAULT_METRICS_DELAY_SECS = 15 * 60;  // 15 minutes.
  static final int DEFAULT_STALE_DATA_DELAY_SECS = 6 * 60 * 60;  // 6 hours.

  // The time after the end time after which a chunk will be marked as read only
  private final int metricsDelay;
  private final int staleDataDelaySecs;
  private ChunkManager chunkManager;

  public OffHeapChunkManagerTask(ChunkManager chunkManager) {
    this(chunkManager, DEFAULT_METRICS_DELAY_SECS, DEFAULT_STALE_DATA_DELAY_SECS);
  }

  public OffHeapChunkManagerTask(ChunkManager chunkManager,
                                 int metricsDelaySecs,
                                 int staleDataDelaySecs) {

    this.chunkManager = chunkManager;
    this.metricsDelay = metricsDelaySecs;
    this.staleDataDelaySecs = staleDataDelaySecs;
  }

  @Override
  public void run() {
    runAt(Instant.now());
  }

  /**
   * Run the chunk manager tasks at an instant. Sometimes some metrics may be very late and will be
   * written to a chunk on the heap. It is wasteful to move this data off heap and then delete it.
   * So, first run deleteStaleData so we can delete stale data before we spend the work to move it
   * off heap. Further, deleting stale data first will also free resources faster.
   */
  @VisibleForTesting
  void runAt(Instant instant) {
    LOG.info("Starting offHeapChunkManagerTask.");
    deleteStaleData(instant);
    detectReadOnlyChunks(instant);
    LOG.info("Finished offHeapChunkManagerTask.");
  }

  @VisibleForTesting
  int detectReadOnlyChunks(Instant startInstant) {
    int secondsToSubtract = this.metricsDelay;
    // cutOffTime = chunk end time + metrics delay.
    final long offHeapCutoffSecs = startInstant.minusSeconds(secondsToSubtract).getEpochSecond();
    return detectChunksPastCutOff(offHeapCutoffSecs);
  }

  @VisibleForTesting
  int detectChunksPastCutOff(long offHeapCutoffSecs) {
    if (offHeapCutoffSecs <= 0) {
      throw new IllegalArgumentException("offHeapCutoffSecs can't be negative.");
    }

    LOG.info("offHeapCutOffSecs is {}", offHeapCutoffSecs);
    List<Map.Entry<Long, Chunk>> readOnlyChunks = new ArrayList<>();
    for (Map.Entry<Long, Chunk> chunkEntry: chunkManager.getChunkMap().entrySet()) {
      Chunk chunk = chunkEntry.getValue();
      if (!chunk.isReadOnly() && offHeapCutoffSecs >= chunk.info().endTimeSecs) {
        readOnlyChunks.add(chunkEntry);
      }
    }

    LOG.info("Number of chunks past cut off: {}.", readOnlyChunks.size());
    chunkManager.toReadOnlyChunks(readOnlyChunks);
    return readOnlyChunks.size();
  }

  /**
   * Delete stale data when running at startInstant.
   */
  int deleteStaleData(Instant startInstant) {
    final long staleCutoffSecs = startInstant.minusSeconds(this.staleDataDelaySecs).getEpochSecond();
    return deleteStaleChunks(staleCutoffSecs);
  }

  /**
   * Delete all chunks that are older than the cut off seconds.
   */
  int deleteStaleChunks(long staleDataCutoffSecs) {
    if (staleDataCutoffSecs <= 0) {
      throw new IllegalArgumentException("staleDateCutOffSecs can't be negative.");
    }

    LOG.info("stale data cut off secs is {}.", staleDataCutoffSecs);
    List<Map.Entry<Long, Chunk>> staleChunks = new ArrayList<>();
    for (Map.Entry<Long, Chunk> chunkEntry: chunkManager.getChunkMap().entrySet()) {
      Chunk chunk = chunkEntry.getValue();
      if (chunk.info().endTimeSecs <= staleDataCutoffSecs) {
        staleChunks.add(chunkEntry);
      }
    }

    LOG.info("Number of stale chunks is: {}.", staleChunks.size());
    chunkManager.removeStaleChunks(staleChunks);
    return staleChunks.size();
  }
}
