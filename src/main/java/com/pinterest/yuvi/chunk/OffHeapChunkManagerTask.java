package com.pinterest.yuvi.chunk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class converts on heap chunk to off heap chunks.
 */
public class OffHeapChunkManagerTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkManager.class);

  // The time after the end time after which a chunk will be marked as read only.
  private static final int CHUNK_READ_ONLY_THRESHOLD_SECS = 15 * 60;  // 15 minutes.

  private ChunkManager chunkManager;

  public OffHeapChunkManagerTask(ChunkManager chunkManager) {
    this.chunkManager = chunkManager;
  }

  @Override
  public void run() {
    final long offHeapCutoffSecs =
        Instant.now().minusSeconds(CHUNK_READ_ONLY_THRESHOLD_SECS).getEpochSecond();

    List<Map.Entry<Long, Chunk>> readOnlyChunks =
        chunkManager.getChunkMap().entrySet().stream()
            .filter(entry -> offHeapCutoffSecs > entry.getValue().info().endTimeSecs)
            .collect(Collectors.toList());

    LOG.info("Number of chunks past cut off are: " + readOnlyChunks.size());
    chunkManager.toReadOnlyChunks(readOnlyChunks);
  }
}
