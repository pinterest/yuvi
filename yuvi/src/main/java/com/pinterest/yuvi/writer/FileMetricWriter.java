package com.pinterest.yuvi.writer;

import com.pinterest.yuvi.chunk.ChunkManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * This class reads metrics from a local file and adds them the chunk. This task only ingests the
 * metrics from the file once.
 */
public class FileMetricWriter implements MetricWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FileMetricWriter.class);

  final Path metricsFilePath;

  final ChunkManager chunkManager;

  public FileMetricWriter(Path metricsFilePath, ChunkManager chunkManager) {
    this.metricsFilePath = metricsFilePath;
    this.chunkManager = chunkManager;
  }

  @Override
  public void start() {
    try {
      try (Stream<String> lines = Files.lines(metricsFilePath, Charset.defaultCharset())) {
        lines.forEachOrdered(line -> {
          if (line == null || line.isEmpty()) { // Ignore empty lines
            return;
          }
          try {
            chunkManager.addMetric(line);
          } catch (Exception e) {
            LOG.info("Error ingesting line {} with exception {}", line, e);
          }
        });
      }
    } catch (IOException e) {
      LOG.info("Caught exception when ingesting metrics from a file", e);
    }
  }

  @Override
  public void close() {

  }

  public ChunkManager getChunkManager() {
    return chunkManager;
  }
}
