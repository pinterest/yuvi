package com.pinterest.yuvi.chunk;

import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.writer.FileMetricWriter;
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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@State(Scope.Thread)
public class ChunkManagerQueryBenchmark {

  private Path filePath = Paths.get(System.getProperty("metricsData"));
  private long startTs = 1489637603L;
  private long endTs = 1489809195L;
  private FileMetricWriter metricWriter;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ChunkManagerQueryBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setup() {
    ChunkManager chunkManager = new ChunkManager("test", 1_000_000);
    metricWriter = new FileMetricWriter(filePath, chunkManager);
    metricWriter.start();
    // Convert all data to offHeap
    chunkManager.toOffHeapChunkMap();
  }

  @Benchmark
  public void queryByMetricName(Blackhole bh) throws Exception {
    List<TimeSeries> timeseries =
        metricWriter.getChunkManager().query(
            Query.parse("tc.proc.stat.cpu.total.coreapp-ngapi-prod"),
            startTs,
            endTs, QueryAggregation.NONE);
    System.out.println("timeseries size: " + timeseries.size());
    System.out.println(
        "points size: " + timeseries.stream().mapToInt(series -> series.getPoints().size()).sum());
    bh.consume(timeseries.size());
  }

  @Benchmark
  public void queryByTag(Blackhole bh) throws Exception {
    List<TimeSeries> timeseries =
        metricWriter.getChunkManager().query(
            Query.parse("tc.proc.stat.cpu.total.coreapp-ngapi-prod ec2_zone=us-east-1d"),
            startTs,
            endTs, QueryAggregation.NONE);
    System.out.println("timeseries size: " + timeseries.size());
    System.out.println(
        "points size: " + timeseries.stream().mapToInt(series -> series.getPoints().size()).sum());
    bh.consume(timeseries.size());
  }

  @Benchmark
  public void queryByWildTag(Blackhole bh) throws Exception {
    List<TimeSeries> timeseries =
        metricWriter.getChunkManager().query(
            Query.parse("tc.proc.stat.cpu.total.coreapp-ngapi-prod host=*"),
            startTs,
            endTs, QueryAggregation.NONE);
    System.out.println("timeseries size: " + timeseries.size());
    System.out.println(
        "points size: " + timeseries.stream().mapToInt(series -> series.getPoints().size()).sum());
    bh.consume(timeseries.size());
  }

  @Benchmark
  public void queryByHost(Blackhole bh) throws Exception {
    List<TimeSeries> timeseries =
        metricWriter.getChunkManager().query(
            Query.parse(
                "tc.proc.stat.cpu.total.coreapp-ngapi-prod host=coreapp-ngapi-prod-0a018feb"),
            startTs,
            endTs, QueryAggregation.NONE);
    System.out.println("timeseries size: " + timeseries.size());
    System.out.println(
        "points size: " + timeseries.stream().mapToInt(series -> series.getPoints().size()).sum());
    bh.consume(timeseries.size());
  }
}
