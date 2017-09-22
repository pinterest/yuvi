package com.pinterest.yuvi.tagstore;

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class InvertedIndexTagStoreBenchmark {

  private static final int numMetrics = 100;
  private static final int numKeys = 4;
  private static final int numValues = 4;

  private List<Metric> metrics;

  @Param({"InvertedIndexTagStore"})
  public String msType;

  private InvertedIndexTagStore ms;

  public static Metric randomMetric(int numMetrics, int numDimensions, int numLabels) {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    int i = r.nextInt(0, numMetrics);
    int[] labs = new int[numDimensions];
    for (int j = 0; j < numDimensions; j++) {
      labs[j] = r.nextInt(0, numLabels);
    }
    return metric(i, labs);
  }

  private static String metricName(int i) {
    return "the.metric.name.is.very.very.very.very.very.long" + i;
  }

  private static Metric metric(int i, int[] labs) {
    List<String> tags = new ArrayList();
    for (int j = 0; j < labs.length; j++) {
      tags.add("key" + j + "=" + "value" + labs[j]);
    }
    return new Metric(metricName(i), tags);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(InvertedIndexTagStoreBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Invocation)
  public void setup() {
    switch (msType) {
      case "InvertedIndexTagStore":
        int initialMapSize = 10000;
        ms = new InvertedIndexTagStore(initialMapSize, initialMapSize);
        metrics = new ArrayList();
        for (int i = 0; i < initialMapSize; i++) {
          metrics.add(randomMetric(numMetrics, numKeys, numValues));
        }
        break;
      default:
        throw new RuntimeException("invalid msType: " + msType);
    }
  }

  @Benchmark
  public void insertMetricThenFetch(Blackhole bh) {
    for (Metric m : metrics) {
      ms.getOrCreate(m);
      bh.consume(ms.get(m));
    }
  }

// TODO: memory used by roaring bitmap. Where is it?
// TODO: Time taken to answer queries as a function of metric size and metric type.
}
