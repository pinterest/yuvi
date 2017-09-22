package com.pinterest.yuvi.chunk;

import com.pinterest.yuvi.utils.MetricUtils;
import com.pinterest.yuvi.metricandtagstore.MetricsAndTagStoreImpl;
import com.pinterest.yuvi.metricstore.VarBitMetricStore;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.InvertedIndexTagStore;
import com.pinterest.yuvi.tagstore.Query;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@State(Scope.Thread)
public class ChunkQueryBenchmark {

  private Chunk chunkStore;
  private Path filePath = Paths.get(System.getProperty("metricsData"));

  private HashMap<String, Integer> counts = new HashMap();

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ChunkQueryBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setup() {
    chunkStore = new ChunkImpl(
        new MetricsAndTagStoreImpl(new InvertedIndexTagStore(1_000_000, 1_000_000), new VarBitMetricStore()),
        null);

    try (Stream<String> lines = Files.lines(filePath, Charset.defaultCharset())) {
      lines.forEachOrdered(line -> {
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
          }
        } catch (Exception e) {
          System.err.println("Error ingesting metric: " + e.getMessage());
          e.printStackTrace();
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Benchmark
  public void queryAllMetrics(Blackhole bh) {
    counts.keySet().stream().forEach(k -> {
      List<TimeSeries> points = chunkStore.query(Query.parse(k));
      bh.consume(points.size());
    });
  }
}
