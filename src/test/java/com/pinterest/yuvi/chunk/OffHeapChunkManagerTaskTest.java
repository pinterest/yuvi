package com.pinterest.yuvi.chunk;

import static com.pinterest.yuvi.chunk.ChunkManager.DEFAULT_CHUNK_DURATION;
import static com.pinterest.yuvi.chunk.OffHeapChunkManagerTask.DEFAULT_METRICS_DELAY_SECS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.pinterest.yuvi.models.Point;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.Query;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Test;

import java.sql.Time;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OffHeapChunkManagerTaskTest {

  private ChunkManager chunkManager;
  private OffHeapChunkManagerTask offHeapChunkManagerTask;

  private final double testValue = 10;
  private final long startTimeSecs = 1488499200;  // Fri, 03 Mar 2017 00:00:00 UTC
  private final long startTimePlusTwoHoursSecs = startTimeSecs + 3600 * 2;
  private final long startTimePlusFourHoursSecs = startTimeSecs + 3600 * 4;
  private final long startTimePlusSixHoursSecs = startTimeSecs + 3600 * 6;

  private final long DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS = DEFAULT_CHUNK_DURATION.getSeconds();

  public long getStartTimePlusTwoHoursSecs() {
    return startTimePlusTwoHoursSecs;
  }

  private final String testMetricName = "testMetric";
  private final String inputTagString1 = "host=h1 dc=dc1";
  private final String inputTagString2 = "host=h2 dc=dc1";

  @Before
  public void setUp() {
    chunkManager = new ChunkManager("test", 1000);
    offHeapChunkManagerTask = new OffHeapChunkManagerTask(chunkManager);
  }

  @Test
  public void testDetectChunksPastCutOff() {
    assertTrue(chunkManager.getChunkMap().isEmpty());
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs + 1));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs + 3600 * 2));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs + 3600 * 3));

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimeSecs + 1, testValue));

    // All stores are on heap
    assertEquals(1, chunkManager.getChunkMap().size());

    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs + 1));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs + 3600 * 2 - 1 ));
    assertEquals(1, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs + 3600 * 2));

    // Expect zero since there are no on-heap chunks.
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs + 3600 * 2));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs + 3600 * 2 + 1));

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimePlusTwoHoursSecs + 1, testValue));

    assertEquals(2, chunkManager.getChunkMap().size());
    assertEquals(1, getReadOnlyChunkCount(chunkManager));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs + 3600 * 3));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS - 2));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS - 1));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + (2 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) - 2));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + (2 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) - 1));
    assertEquals(1, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + (2 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS)));
    assertEquals(2, chunkManager.getChunkMap().size());
    assertEquals(2, getReadOnlyChunkCount(chunkManager));

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimePlusFourHoursSecs + 1, testValue));
    assertEquals(3, chunkManager.getChunkMap().size());
    assertEquals(2, getReadOnlyChunkCount(chunkManager));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs + 3600 * 3));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS - 2));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS - 1));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + (2 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) - 2));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + (2 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) - 1));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + (2 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS)));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + 5 * 3600));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + (3 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) - 2));
    assertEquals(0, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + (3 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) - 1));
    assertEquals(1, offHeapChunkManagerTask.detectChunksPastCutOff(
        startTimeSecs + (3 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS)));
    assertEquals(3, chunkManager.getChunkMap().size());
    assertEquals(3, getReadOnlyChunkCount(chunkManager));
  }

  @Test
  public void testDetectReadOnlyChunksWithDefaultValues() {
    assertTrue(chunkManager.getChunkMap().isEmpty());
    assertEquals(0, detectReadOnlyChunks(startTimeSecs));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs + 1));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs + 3600 * 2));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs + 3600 * 3));

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimeSecs + 1, testValue));

    assertEquals(1, chunkManager.getChunkMap().size());
    assertEquals(0, detectReadOnlyChunks(startTimeSecs));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS - 1));
    assertEquals(0,
        detectReadOnlyChunks(startTimeSecs + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + 1));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS - 1));
    assertEquals(1, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS));
    assertEquals(1, chunkManager.getChunkMap().size());
    assertEquals(1, getReadOnlyChunkCount(chunkManager));

    // Expect zero since there are no on-heap chunks.
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS + 1));

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimePlusTwoHoursSecs + 1, testValue));
    assertEquals(2, chunkManager.getChunkMap().size());
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        +  DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS + 3600));
    assertEquals(1, detectReadOnlyChunks(startTimeSecs
        + 2 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS));
    assertEquals(2, getReadOnlyChunkCount(chunkManager));
    assertEquals(2, chunkManager.getChunkMap().size());

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimePlusFourHoursSecs + 1, testValue));
    assertEquals(3, chunkManager.getChunkMap().size());
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS));
    assertEquals(0, detectReadOnlyChunks(startTimePlusTwoHoursSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + DEFAULT_METRICS_DELAY_SECS));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + (2 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) + DEFAULT_METRICS_DELAY_SECS));
    assertEquals(1, detectReadOnlyChunks(startTimeSecs
        + (3 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) + DEFAULT_METRICS_DELAY_SECS));
    assertEquals(3, getReadOnlyChunkCount(chunkManager));
    assertEquals(3, chunkManager.getChunkMap().size());
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + (3 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) + DEFAULT_METRICS_DELAY_SECS));
  }

  @Test
  public void testDetectReadOnlyChunksWithMetricsDelay() {
    final int metricsDelaySecs = 5 * 60;
    offHeapChunkManagerTask = new OffHeapChunkManagerTask(chunkManager, metricsDelaySecs);

    assertTrue(chunkManager.getChunkMap().isEmpty());
    assertEquals(0, detectReadOnlyChunks(startTimeSecs));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs + 1));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs + 3600 * 2));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs + 3600 * 3));

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimeSecs + 1, testValue));

    assertEquals(1, chunkManager.getChunkMap().size());
    assertEquals(0, detectReadOnlyChunks(startTimeSecs));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS - 1));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + 1));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + metricsDelaySecs - 1));
    assertEquals(1, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + metricsDelaySecs));
    assertEquals(1, chunkManager.getChunkMap().size());
    assertEquals(1, getReadOnlyChunkCount(chunkManager));

    // Expect zero since there are no on-heap chunks.
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + metricsDelaySecs + 1));

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimePlusTwoHoursSecs + 1, testValue));
    assertEquals(2, chunkManager.getChunkMap().size());
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        +  DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + metricsDelaySecs));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + metricsDelaySecs + 3600));
    assertEquals(1, detectReadOnlyChunks(startTimeSecs
        + 2 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + metricsDelaySecs));
    assertEquals(2, getReadOnlyChunkCount(chunkManager));
    assertEquals(2, chunkManager.getChunkMap().size());

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimePlusFourHoursSecs + 1, testValue));
    assertEquals(3, chunkManager.getChunkMap().size());
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + metricsDelaySecs));
    assertEquals(0, detectReadOnlyChunks(startTimePlusTwoHoursSecs
        + DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS + metricsDelaySecs));
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + (2 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) + metricsDelaySecs));
    assertEquals(1, detectReadOnlyChunks(startTimeSecs
        + (3 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) + metricsDelaySecs));
    assertEquals(3, getReadOnlyChunkCount(chunkManager));
    assertEquals(3, chunkManager.getChunkMap().size());
    assertEquals(0, detectReadOnlyChunks(startTimeSecs
        + (3 * DEFAULT_CHUNK_READ_ONLY_THRESHOLD_SECS) + metricsDelaySecs));
  }

  private long getReadOnlyChunkCount(ChunkManager chunkManager) {
    return chunkManager.getChunkMap().values().stream().filter(c -> c.isReadOnly()).count();
  }

  private int detectReadOnlyChunks(long startTimeSecs) {
    return offHeapChunkManagerTask.detectReadOnlyChunks(Instant.ofEpochSecond(startTimeSecs));
  }

  @Test(expected =  IllegalArgumentException.class)
  public void testInsertionIntoOffHeapStore() {
    assertTrue(chunkManager.getChunkMap().isEmpty());

    // All stores are on heap
    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimeSecs + 1, testValue));

    assertEquals(1, chunkManager.getChunkMap().size());

    // All stores are off heap.
    assertEquals(1, offHeapChunkManagerTask.detectChunksPastCutOff(startTimeSecs + 3600 * 2));
    assertEquals(1, chunkManager.getChunkMap().size());
    assertEquals(1, getReadOnlyChunkCount(chunkManager));

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimeSecs + 2, testValue * 2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeOffset() {
    offHeapChunkManagerTask.detectChunksPastCutOff(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testZeroOffset() {
    offHeapChunkManagerTask.detectChunksPastCutOff(0);
  }

  @Test
  public void testOffHeapMoveAtChunkManager() {
    final String expectedMetricName1 = testMetricName + " dc=dc1 host=h1";
    final String expectedMetricName2 = testMetricName + " dc=dc1 host=h2";

    // Empty map
    assertTrue(chunkManager.getChunkMap().isEmpty());
    final List<TimeSeries> timeSeries = queryChunkManager(startTimeSecs, startTimeSecs + 3600 * 3);
    assertTrue(timeSeries.isEmpty());

    // Test one chunk
    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimeSecs + 1, testValue));
    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString2, startTimeSecs + 1, testValue));

    // query all on-heap
    final List<TimeSeries> timeSeries1 = queryChunkManager(startTimeSecs, startTimeSecs + 3600 * 3);
    assertEquals(2, timeSeries1.size());
    final TimeSeries expectedTimeSeries1 = new TimeSeries(expectedMetricName1,
        Arrays.asList(new Point(startTimeSecs + 1, testValue)));
    final TimeSeries expectedTimeSeries12 = new TimeSeries(expectedMetricName2,
        Arrays.asList(new Point(startTimeSecs + 1, testValue)));
    assertTimeSeries(expectedTimeSeries1, expectedTimeSeries12, timeSeries1);

    // query all on-heap
    moveOffHeapAndCheck(1, 1);

    // Query after moving to off heap
    final List<TimeSeries> timeSeries2 = queryChunkManager(startTimeSecs, startTimeSecs + 3600 * 3);
    assertEquals(2, timeSeries2.size());
    assertTimeSeries(expectedTimeSeries1, expectedTimeSeries12, timeSeries2);

    // Test with two chunks
    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimePlusTwoHoursSecs + 1, testValue * 2));
    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString2, startTimePlusTwoHoursSecs + 1, testValue * 2));

    // Query mixed on heap and off heap
    final List<TimeSeries> timeSeries3 =
        queryChunkManager(startTimeSecs, startTimePlusFourHoursSecs);
    assertEquals(2, timeSeries3.size());
    final TimeSeries expectedTimeSeries2 = new TimeSeries(expectedMetricName1,
        Arrays.asList(new Point(startTimeSecs + 1, testValue),
            new Point(startTimePlusTwoHoursSecs + 1, testValue * 2)));
    final TimeSeries expectedTimeSeries22 = new TimeSeries(expectedMetricName2,
        Arrays.asList(new Point(startTimeSecs + 1, testValue),
            new Point(startTimePlusTwoHoursSecs + 1, testValue * 2)));
    assertTimeSeries(expectedTimeSeries2, expectedTimeSeries22, timeSeries3);

    moveOffHeapAndCheck(2, 1);

    // query multiple chunks off heap
    final List<TimeSeries> timeSeries4 =
        queryChunkManager(startTimeSecs, startTimePlusFourHoursSecs);
    assertEquals(2, timeSeries4.size());
    assertTimeSeries(expectedTimeSeries2, expectedTimeSeries22, timeSeries4);

    // Test with 3 chunks
    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimePlusFourHoursSecs + 1, testValue * 3));
    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString2, startTimePlusFourHoursSecs + 1, testValue * 3));

    final List<TimeSeries> timeSeries5 =
        queryChunkManager(startTimeSecs, startTimePlusSixHoursSecs);
    assertEquals(2, timeSeries5.size());
    final TimeSeries expectedTimeSeries3 = new TimeSeries(expectedMetricName1,
        Arrays.asList(new Point(startTimeSecs + 1, testValue),
            new Point(startTimePlusTwoHoursSecs + 1, testValue * 2),
            new Point(startTimePlusFourHoursSecs + 1 , testValue * 3)));
    final TimeSeries expectedTimeSeries32 = new TimeSeries(expectedMetricName2,
        Arrays.asList(new Point(startTimeSecs + 1, testValue),
            new Point(startTimePlusTwoHoursSecs + 1, testValue * 2),
            new Point(startTimePlusFourHoursSecs + 1 , testValue * 3)));
    assertTimeSeries(expectedTimeSeries3, expectedTimeSeries32, timeSeries5);

    moveOffHeapAndCheck(3, 1);

    // query multiple off heap
    final List<TimeSeries> timeSeries6 =
        queryChunkManager(startTimeSecs, startTimePlusSixHoursSecs);
    assertEquals(2, timeSeries6.size());
    assertTimeSeries(expectedTimeSeries3, expectedTimeSeries32, timeSeries6);
  }

  private void assertTimeSeries(TimeSeries expectedTimeSeries1,
                                TimeSeries expectedTimeSeries2,
                                List<TimeSeries> actualTimeSeries) {

    assertThat(actualTimeSeries, IsIterableContainingInAnyOrder.containsInAnyOrder(
        new Object[]{expectedTimeSeries1, expectedTimeSeries2}));
  }

  private List<TimeSeries> queryChunkManager(long startTimeSecs, long endTimeSecs) {
    return chunkManager.query(new Query(testMetricName, Collections.emptyList()),
        startTimeSecs, endTimeSecs, QueryAggregation.NONE);
  }

  private void moveOffHeapAndCheck(int expectedSize, int expectedReadOnlyChunksSize) {
    assertEquals(expectedSize, chunkManager.getChunkMap().size());
    int readOnlyChunksSize = offHeapChunkManagerTask.detectChunksPastCutOff(1488585600);
    // All stores are off heap
    assertEquals(expectedReadOnlyChunksSize, readOnlyChunksSize);
    assertEquals(expectedSize, chunkManager.getChunkMap().size());
    assertEquals(expectedSize, getReadOnlyChunkCount(chunkManager));
  }
}
