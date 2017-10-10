package com.pinterest.yuvi.chunk;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Yuvi stores all the metrics data in memory, but there is a GC overhead to storing all the metrics
 * data in memory, so we move the metrics data that will be no longer updated to off heap store and
 * mark it read only to save GC overhead.
 *
 * This class identifies chunks in chunk manager that can be moved off heap. A chunk can be moved
 * off heap if the current time is greater than the chunk end time. However, sometimes metrics may
 * be delayed because of transient issues. To ingest late arriving metrics, we wait a metricsDelay
 * amount of time before we start marking the chunk read-only and moving it to off heap.
 */
public class OffHeapChunkManagerTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkManager.class);

  static final int DEFAULT_METRICS_DELAY_SECS = 15 * 60;  // 15 minutes.

  // The time after the end time after which a chunk will be marked as read only
  private final int metricsDelay;

  private ChunkManager chunkManager;

  public OffHeapChunkManagerTask(ChunkManager chunkManager) {
    this(chunkManager, DEFAULT_METRICS_DELAY_SECS);
  }

  public OffHeapChunkManagerTask(ChunkManager chunkManager, int metricsDelay) {
    this.chunkManager = chunkManager;
    this.metricsDelay = metricsDelay;
  }

  @Override
  public void run() {
    LOG.info("Starting offHeapChunkManagerTask.");
    detectReadOnlyChunks(Instant.now());
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
}
