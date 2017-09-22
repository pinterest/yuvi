package com.pinterest.yuvi.chunk;

import com.pinterest.yuvi.utils.MetricUtils;
import com.pinterest.yuvi.metricandtagstore.MetricsAndTagStoreImpl;
import com.pinterest.yuvi.metricstore.VarBitMetricStore;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.InvertedIndexTagStore;
import com.pinterest.yuvi.tagstore.Query;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

/**
 * This program takes a raw metrics file as input, ingests the data in it into a Yuvi Chunk,
 * reads it back and compares it to input data.
 *
 * For supporting large files, it compares the number of data points per metric instead of
 * actual data points.
 */
public class ChunkDataIntegrityChecker {

  private final Path filePath;
  private final int tagStoreSize;

  public ChunkDataIntegrityChecker() {
    this(System.getProperty("metricsData"), System.getProperty("tagStoreSize", "1000000"));
  }

  public ChunkDataIntegrityChecker(String filePathName, String tagStoreSize) {
    if (filePathName == null || filePathName.isEmpty()) {
      throw new IllegalArgumentException("filePathName can't be empty");
    }

    filePath = Paths.get(filePathName);
    this.tagStoreSize = new Integer(tagStoreSize);
  }

  public static void main(String[] args) throws IOException {
    ChunkDataIntegrityChecker checker = new ChunkDataIntegrityChecker();
    checker.checkData();
  }

  void checkData() throws IOException {
    Chunk chunkStore = new ChunkImpl(
        new MetricsAndTagStoreImpl(new InvertedIndexTagStore(tagStoreSize, tagStoreSize), new VarBitMetricStore()),
        null);

    HashMap<String, Integer> counts = new HashMap();

    try (Stream<String> lines = Files.lines(filePath, Charset.defaultCharset())) {
      lines.forEachOrdered(line -> {
        if (line == null || line.isEmpty()) {
          return;
        }

        try {
          String[] words = line.split(" ");
          String metricName = words[1];
          if (metricName != null && !metricName.isEmpty()) {
            if (counts.containsKey(metricName)) {
              counts.put(metricName, counts.get(metricName) + 1);
            } else {
              counts.put(metricName, 1);
            }

            MetricUtils.parseAndAddOpenTsdbMetric(line, chunkStore);
          } else {
            System.out.println(metricName + " " + line);
          }
        } catch (Exception e) {
          System.out.println("error: " + e.getMessage());
          e.printStackTrace();
        }
      });
    }

    final int[] stats = new int[3];
    stats[0] = 0;
    stats[1] = 0;
    counts.keySet().stream().forEach(k -> {
      try {
        List<TimeSeries> timeSeriesList = chunkStore.query(Query.parse(k));
        int pointsCount = timeSeriesList.stream().mapToInt(l -> l.getPoints().size()).sum();
        if (counts.get(k) != pointsCount) {
          stats[2] = stats[2] + 1;
        }
        stats[1] = stats[1] + 1;
      } catch (Exception e) {
        stats[0] = stats[0] + 1;
      }
    });

    System.out.println("Skipped metrics (should be zero): " + stats[0]);
    System.out.println("Mismatched points count (should be zero): " + stats[2]);
    System.out.println("Processed metrics (should be non-zero): " + stats[1]);
  }
}
