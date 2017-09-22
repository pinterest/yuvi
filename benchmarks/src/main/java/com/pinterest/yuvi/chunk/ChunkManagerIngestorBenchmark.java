package com.pinterest.yuvi.chunk;

import com.pinterest.yuvi.writer.FileMetricWriter;

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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class ChunkManagerIngestorBenchmark {

  private ChunkManager chunkManager;
  private Path filePath = Paths.get(System.getProperty("metricsData"));

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ChunkManagerIngestorBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }

  @Setup(Level.Invocation)
  public void setup() {
    chunkManager = new ChunkManager("test", 1_000_000);
  }

  @Benchmark
  public void fileIngestTcollectorMetrics(Blackhole bh) throws Exception {
    FileMetricWriter metricWriter = new FileMetricWriter(filePath, chunkManager);
    metricWriter.start();
    bh.consume(metricWriter);
  }
}
