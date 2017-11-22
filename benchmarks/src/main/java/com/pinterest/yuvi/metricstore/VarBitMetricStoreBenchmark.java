package com.pinterest.yuvi.metricstore;

import com.pinterest.yuvi.tagstore.Metric;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.MICROSECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.MICROSECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class VarBitMetricStoreBenchmark {

  private Path filePath = Paths.get(System.getProperty("metricsData"));
  final int[] metricCounter = {0};

  HashMap<String, Integer> metricidHashMap = new HashMap();
  VarBitMetricStore store = new VarBitMetricStore(10000000);

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(VarBitMetricStoreBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }

  @Param({"1", "5", "10", "50", "100", "500", "1000", "5000", "10000"})
  public int fetchCount;


  @Setup
  public void setup() {
    try {
      load();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Benchmark
  public void fetch(Blackhole bh) {
    bh.consume(fetchN(2 + fetchCount));
  }

  private int fetchN(int n) {
    int size = 0;
    for (int i = 2; i < n; i++) {
      size = size + store.getSeries(2).size();
    }
    return size;
  }

  public void load() throws IOException {
    // Get metric name and put it in a map and assign it a number.
    // Add ts and value to metric store with that number.
    try (Stream<String> lines = Files.lines(filePath, Charset.defaultCharset())) {
      lines.forEachOrdered(line -> {
        if (line == null || line.isEmpty()) { // Ignore empty lines
          return;
        }
        try {
          String[] metricParts = line.split(" ");
          if (metricParts.length > 1 && metricParts[0].equals("put")) {
            String metricName = metricParts[1].trim();
            List<String> rawTags = Arrays.asList(metricParts).subList(4, metricParts.length);
            Metric metric = new Metric(metricName, rawTags);
            long ts = Long.parseLong(metricParts[2].trim());
            double value = Double.parseDouble(metricParts[3].trim());

            // System.out.println(metric.fullMetricName);

            int id = -1;
            if (metricidHashMap.containsKey(metric.fullMetricName)) {
              id = metricidHashMap.get(metric.fullMetricName);
            } else {
              metricCounter[0] = metricCounter[0] + 1;
              metricidHashMap.put(metric.fullMetricName, metricCounter[0]);
              id = metricCounter[0];
            }

            store.addPoint(id, ts, value);
          }
        } catch (Exception e) {
          // System.out.println("Error ingesting line " + line + " with exception " + e.getMessage());
        }
      });
    }
    metricidHashMap.clear();
    // System.out.println("Metric counter size: " + metricCounter[0]);
  }
}
