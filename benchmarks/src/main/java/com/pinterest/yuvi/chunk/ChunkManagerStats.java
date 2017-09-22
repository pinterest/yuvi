package com.pinterest.yuvi.chunk;

import com.pinterest.yuvi.chunk.Chunk;
import com.pinterest.yuvi.chunk.ChunkManager;
import com.pinterest.yuvi.writer.FileMetricWriter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class ChunkManagerStats {

  public static void main(String[] args) {
    Path filePath = Paths.get(System.getProperty("metricsData"));
    int tagStoreSize = new Integer(System.getProperty("tagStoreSize"));
    ChunkManager chunkManager = new ChunkManager("test", tagStoreSize);
    FileMetricWriter metricWriter = new FileMetricWriter(filePath, chunkManager);
    metricWriter.start();

    Map<Long, Chunk> chunkMap = chunkManager.getChunkMap();
    chunkMap.values().forEach(s -> System.out.println(s.getStats()));
  }
}
