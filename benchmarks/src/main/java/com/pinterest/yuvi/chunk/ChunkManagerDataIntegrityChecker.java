package com.pinterest.yuvi.chunk;

import com.pinterest.yuvi.models.TimeSeries;
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
 * This program takes a raw metrics file as input, ingests the data in it into a Yuvi ChunkManager,
 * reads it back and compares it to input data.
 *
 * For supporting large files, it compares the number of data points per metric instead of
 * actual data points.
 */
public class ChunkManagerDataIntegrityChecker {

  private final Path filePath;
  private final int tagStoreSize;
  private final boolean printErrorKey;

  public ChunkManagerDataIntegrityChecker() {
    this(System.getProperty("metricsData"), System.getProperty("tagStoreSize", "1000000"));
  }

  public ChunkManagerDataIntegrityChecker(String filePathName, String tagStoreSize) {
    if (filePathName == null || filePathName.isEmpty()) {
      throw new IllegalArgumentException("filePathName can't be empty");
    }

    filePath = Paths.get(filePathName);
    this.tagStoreSize = new Integer(tagStoreSize);
    this.printErrorKey = new Boolean(System.getProperty("printErrorKey"));
  }

  public static void main(String[] args) throws IOException {
    ChunkManagerDataIntegrityChecker checker = new ChunkManagerDataIntegrityChecker();
    final int[] stats = checker.checkData();
    if (stats[0] == 0 && stats[2] == 0) {
      System.exit(0);
    } else {
      System.exit(-1);
    }
  }

  int[] checkData() throws IOException {
    ChunkManager store = new ChunkManager("test", tagStoreSize);

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
            store.addMetric(line);
            if (counts.containsKey(metricName)) {
              counts.put(metricName, counts.get(metricName) + 1);
            } else {
              counts.put(metricName, 1);
            }
          } else {
            System.out.println(metricName + " " + line);
          }
        } catch (Exception e) {
          System.out.println("error: " + e.getMessage());
          e.printStackTrace();
        }
      });
    }

    checkMetrics(store, counts);

    store.toOffHeapChunkMap();
    System.out.println("Converted to off heap store");
    final int[] offHeapStats = checkMetrics(store, counts);

    return offHeapStats;
  }

  private int[] checkMetrics(ChunkManager store, HashMap<String, Integer> counts) {
    final int[] stats = new int[3];
    stats[0] = 0;
    stats[1] = 0;
    counts.keySet().stream().forEach(k -> {
      try {
        List<TimeSeries>
            timeSeriesList = store.query(Query.parse(k), 0, Long.MAX_VALUE, QueryAggregation.NONE);
        int pointsCount = timeSeriesList.stream().mapToInt(l -> l.getPoints().size()).sum();
        if (counts.get(k) != pointsCount) {
          stats[2] = stats[2] + 1;
          if (printErrorKey) {
            System.out.println("Error key: " + k);
          }
        }
        stats[1] = stats[1] + 1;
      } catch (Exception e) {
        stats[0] = stats[0] + 1;
      }
    });

    System.out.println("Skipped metrics (should be zero): " + stats[0]);
    System.out.println("Mismatched points count (should be zero): " + stats[2]);
    System.out.println("Processed metrics (should be non-zero): " + stats[1]);
    return stats;
  }
}
