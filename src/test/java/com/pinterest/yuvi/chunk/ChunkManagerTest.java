package com.pinterest.yuvi.chunk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.pinterest.yuvi.models.Point;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.Query;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ChunkManagerTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private ChunkManager chunkManager;

  private final long testTs = 100L;
  private final double testValue = 10;

  private final String testMetricName = "testMetric";
  private final String inputTagString = "host=h1 dc=dc1";
  private final String expectedMetricName = testMetricName + " dc=dc1 host=h1";

  private final long startTime = 1488499200;  // Fri, 03 Mar 2017 00:00:00 UTC
  private final long startTimePlusTwoHours = startTime + 3600 * 2;
  private final long startTimePlusFourHours = startTime + 3600 * 4;

  private final String inputTagString1 = "host=h1 dc=dc1";

  public static long getReadOnlyChunkCount(ChunkManager chunkManager) {
    return chunkManager.getChunkMap().values().stream().filter(c -> c.isReadOnly()).count();
  }

  @Before
  public void setUp() {
    chunkManager = new ChunkManager("test", 1000);
  }

  @Test
  public void testChunkCreation() {
    assertTrue(chunkManager.getChunkMap().isEmpty());

    Chunk testChunk1 = chunkManager.getChunk(startTime + 1);
    assertTrue(testChunk1.info().dataSet.startsWith("test"));
    assertTrue(testChunk1.info().dataSet.contains(Long.toString(startTime)));
    assertEquals(startTime, testChunk1.info().startTimeSecs);
    assertEquals(startTime + ChunkManager.DEFAULT_CHUNK_DURATION.getSeconds(),
        testChunk1.info().endTimeSecs);

    assertEquals(1, chunkManager.getChunkMap().size());
    assertTrue(chunkManager.getChunkMap().containsKey(startTime));
    assertEquals(testChunk1, chunkManager.getChunk(startTime));
    checkSameChunkReturnedForNextTwoHours(testChunk1, startTime);

    long startTimePlusTwoHours = startTime + 3600 * 2;
    Chunk testChunk2 = chunkManager.getChunk(startTimePlusTwoHours + 1);
    assertTrue(testChunk2.info().dataSet.startsWith("test"));
    assertTrue(testChunk2.info().dataSet.contains(Long.toString(startTimePlusTwoHours)));
    assertEquals(startTimePlusTwoHours, testChunk2.info().startTimeSecs);
    assertEquals(startTimePlusTwoHours + ChunkManager.DEFAULT_CHUNK_DURATION.getSeconds(),
        testChunk2.info().endTimeSecs);

    assertEquals(2, chunkManager.getChunkMap().size());
    assertTrue(chunkManager.getChunkMap().containsKey(startTimePlusTwoHours));
    assertTrue(chunkManager.getChunkMap().containsKey(startTime));
    assertEquals(testChunk2, chunkManager.getChunk(startTimePlusTwoHours));
    assertEquals(testChunk1, chunkManager.getChunk(startTime));
    checkSameChunkReturnedForNextTwoHours(testChunk1, startTime);
    checkSameChunkReturnedForNextTwoHours(testChunk2, startTimePlusTwoHours);

    for (int i = 3; i < 13; i++) {
      chunkManager.getChunk(startTime + (3600 * 2 * i) + 1);
    }
    assertEquals(12, chunkManager.getChunkMap().size());
  }

  private void checkSameChunkReturnedForNextTwoHours(Chunk testChunk, long startTime) {
    assertEquals(testChunk, chunkManager.getChunk(startTime + 1));
    assertEquals(testChunk, chunkManager.getChunk(startTime + 10));
    assertEquals(testChunk, chunkManager.getChunk(startTime + 3600));
    assertEquals(testChunk, chunkManager.getChunk(startTime + 3600 * 2 - 1));
  }

  @Test
  public void testChunkWithRawMetricData() {
    long currentTs = Instant.now().getEpochSecond();
    long previousHourTs = Instant.now().minusSeconds(3600).getEpochSecond();

    // query empty store.
    final Query emptyQuery = new Query("test", Collections.emptyList());
    assertTrue(
        chunkManager.query(emptyQuery, currentTs, previousHourTs, QueryAggregation.NONE).isEmpty());
  }

  @Test
  public void testMultipleChunkQuery() throws ReadOnlyChunkInsertionException {
    assertTrue(chunkManager.getChunkMap().isEmpty());

    // 1 data point per chunk, 1 metric
    chunkManager.addMetric(
        MetricUtils.makeMetricString(testMetricName, inputTagString, startTime + 1, testValue));
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName, inputTagString, startTimePlusTwoHours + 1, testValue));

    assertEquals(2, chunkManager.getChunkMap().size());

    // Query for 3 hours worth of data
    List<TimeSeries> timeSeries = chunkManager.query(
        new Query(testMetricName, Collections.emptyList()),
        startTime,
        startTime + 3600 * 3, QueryAggregation.NONE);
    assertEquals(1, timeSeries.size());
    assertEquals(expectedMetricName, timeSeries.get(0).getMetric());
    assertEquals(2, timeSeries.get(0).getPoints().size());

    final TimeSeries expectedTimeSeries1 = new TimeSeries(expectedMetricName,
        Arrays.asList(new Point(startTime + 1, testValue),
            new Point(startTimePlusTwoHours + 1, testValue)));

    assertThat(timeSeries,
        IsIterableContainingInAnyOrder.containsInAnyOrder(expectedTimeSeries1));

    // Add a point outside range and run same query.
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName, inputTagString, startTime + (3600 * 4) + 1, testValue));
    assertEquals(3, chunkManager.getChunkMap().size());

    List<TimeSeries> timeSeries2 = chunkManager.query(
        new Query(testMetricName, Collections.emptyList()),
        startTime,
        startTime + 3600 * 3, QueryAggregation.NONE);
    assertEquals(1, timeSeries2.size());
    assertEquals(expectedMetricName, timeSeries2.get(0).getMetric());
    assertEquals(2, timeSeries2.get(0).getPoints().size());

    assertThat(timeSeries2,
        IsIterableContainingInAnyOrder.containsInAnyOrder(expectedTimeSeries1));

    // Add different metrics to same chunk
    String additionalTag = " instance=1";
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName, inputTagString + additionalTag, startTime + 1,
            testValue * 2));

    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName, inputTagString + additionalTag, startTimePlusTwoHours + 1,
            testValue * 3));

    List<TimeSeries> timeSeries3 = chunkManager.query(
        new Query(testMetricName, Collections.emptyList()),
        startTime,
        startTime + 3600 * 3, QueryAggregation.NONE);
    assertEquals(2, timeSeries3.size());
    assertEquals(expectedMetricName + additionalTag, timeSeries3.get(0).getMetric());
    assertEquals(2, timeSeries3.get(0).getPoints().size());
    final TimeSeries expectedTimeSeries2 = new TimeSeries(expectedMetricName + additionalTag,
        Arrays.asList(new Point(startTime + 1, testValue * 2),
            new Point(startTimePlusTwoHours + 1, testValue * 3)));

    assertThat(timeSeries3, IsIterableContainingInAnyOrder.containsInAnyOrder(
        expectedTimeSeries1, expectedTimeSeries2));

    // Query the data by a tag
    List<TimeSeries> timeSeries4 = chunkManager.query(
        Query.parse(testMetricName + additionalTag),
        startTime,
        startTime + 3600 * 3, QueryAggregation.NONE);
    assertEquals(1, timeSeries4.size());
    assertEquals(expectedMetricName + additionalTag, timeSeries4.get(0).getMetric());
    assertEquals(2, timeSeries4.get(0).getPoints().size());
    assertThat(timeSeries4, IsIterableContainingInAnyOrder.containsInAnyOrder(
        expectedTimeSeries2));

    // Add a duplicate point and query it. Duplicate points are not allowed.
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName, inputTagString + additionalTag, startTime + 1,
            testValue * 2));
    List<TimeSeries> timeSeries5 = chunkManager.query(
        Query.parse(testMetricName + additionalTag),
        startTime,
        startTime + 3600 * 3, QueryAggregation.NONE);
    assertEquals(1, timeSeries5.size());
    assertEquals(expectedMetricName + additionalTag, timeSeries5.get(0).getMetric());
    assertEquals(2, timeSeries5.get(0).getPoints().size());
    final TimeSeries expectedTimeSeries3 = new TimeSeries(expectedMetricName + additionalTag,
        Arrays.asList(new Point(startTime + 1, testValue * 2),
            new Point(startTimePlusTwoHours + 1, testValue * 3)));

    assertThat(timeSeries5, IsIterableContainingInAnyOrder.containsInAnyOrder(
        expectedTimeSeries3));

    // Add a different metric name and query it.
    final String testMetricName1 = "testMetric1";
    final String expectedMetricName1 = "testMetric1" + " dc=dc1 host=h1";
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName1, inputTagString, startTime + 2, testValue));
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName1, inputTagString, startTimePlusTwoHours + 2, testValue));
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName1, inputTagString, startTime + (3600 * 4) + 2,
            testValue));
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName1, inputTagString + additionalTag, startTime + 2,
            testValue * 2));
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName1, inputTagString + additionalTag,
            startTimePlusTwoHours + 2, testValue * 3));

    List<TimeSeries> timeSeries6 = chunkManager.query(
        Query.parse(testMetricName1),
        startTime,
        startTime + 3600 * 3, QueryAggregation.NONE);
    assertEquals(2, timeSeries6.size());
    final TimeSeries expectedTimeSeries4 = new TimeSeries(expectedMetricName1,
        Arrays.asList(new Point(startTime + 2, testValue),
            new Point(startTimePlusTwoHours + 2, testValue)));

    final TimeSeries expectedTimeSeries5 = new TimeSeries(expectedMetricName1 + additionalTag,
        Arrays.asList(new Point(startTime + 2, testValue * 2),
            new Point(startTimePlusTwoHours + 2, testValue * 3)));

    assertThat(timeSeries6, IsIterableContainingInAnyOrder.containsInAnyOrder(
        expectedTimeSeries4, expectedTimeSeries5));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMetricName() throws ReadOnlyChunkInsertionException {
    chunkManager.addMetric("random");
  }

  @Test
  public void testMetricMissingTags() throws ReadOnlyChunkInsertionException {
    String metric = "put a.b.c.d-e 1465530393 0";
    chunkManager.addMetric(metric);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMetricInvalidTs() throws ReadOnlyChunkInsertionException {
    String metric = "put a.b.c.d-e 1465530393a 0";
    chunkManager.addMetric(metric);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMetricInvalidValue() throws ReadOnlyChunkInsertionException {
    String metric = "put a.b.c.d-e 1465530393 a0";
    chunkManager.addMetric(metric);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMetricInvalidTag() throws ReadOnlyChunkInsertionException {
    String metric = "put a.b.c.d-e 1465530393 0 a";
    chunkManager.addMetric(metric);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingMetricName() throws ReadOnlyChunkInsertionException {
    String metric = "put 1465530393 0";
    chunkManager.addMetric(metric);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingValue() throws ReadOnlyChunkInsertionException {
    String metric = "put a.b 1465530393 c=d";
    chunkManager.addMetric(metric);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingTs() throws ReadOnlyChunkInsertionException {
    String metric = "put a.b 5.1 c=d";
    chunkManager.addMetric(metric);
  }

  // TODO: Currently, toReadOnlyChunks and toOffHeapChunk is tested by OffHeapChunkManager.
  // Add some tests here also.

  @Test
  public void testRemoveStaleChunks() throws ReadOnlyChunkInsertionException {
    final Map.Entry<Long, Chunk> fakeMapEntry = new Map.Entry<Long, Chunk>() {
      @Override
      public Long getKey() {
        return 100L;
      }

      @Override
      public Chunk getValue() {
        return null;
      }

      @Override
      public Chunk setValue(Chunk value) {
        return null;
      }
    };

    assertTrue(chunkManager.getChunkMap().isEmpty());
    // Delete an entry from an empty map.
    chunkManager.removeStaleChunks(Collections.emptyList());
    chunkManager.removeStaleChunks(Collections.singletonList(fakeMapEntry));
    assertTrue(chunkManager.getChunkMap().isEmpty());

    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName, inputTagString, startTime + 1, testValue));

    assertEquals(1, chunkManager.getChunkMap().size());
    ArrayList<Map.Entry<Long, Chunk>> chunks = new ArrayList<>(chunkManager.getChunkMap().entrySet());
    chunkManager.removeStaleChunks(chunks);
    assertTrue(chunkManager.getChunkMap().isEmpty());

    // Remove 2 chunks
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName, inputTagString, startTime + 1, testValue));
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName, inputTagString, startTimePlusTwoHours + 1, testValue));
    assertEquals(2, chunkManager.getChunkMap().size());
    ArrayList<Map.Entry<Long, Chunk>> chunks2 = new ArrayList<>(chunkManager.getChunkMap().entrySet());
    chunkManager.removeStaleChunks(chunks2);
    assertTrue(chunkManager.getChunkMap().isEmpty());

    // Remove 2 of 3 chunks
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName, inputTagString, startTime + 1, testValue));
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName, inputTagString, startTimePlusTwoHours, testValue));
    chunkManager.addMetric(MetricUtils
        .makeMetricString(testMetricName, inputTagString, startTimePlusFourHours, testValue));
    assertEquals(3, chunkManager.getChunkMap().size());
    ArrayList<Map.Entry<Long, Chunk>> chunks3 = new ArrayList<>();
    for (Map.Entry<Long, Chunk> entry : chunkManager.getChunkMap().entrySet()) {
      if (entry.getKey() < startTimePlusFourHours) {
        chunks3.add(entry);
      }
    }
    chunkManager.removeStaleChunks(chunks3);
    assertEquals(1, chunkManager.getChunkMap().size());
    assertEquals(startTimePlusFourHours,
        chunkManager.getChunkMap().get(startTimePlusFourHours).info().startTimeSecs);

    // Delete a non-existent chunk.
    chunkManager.removeStaleChunks(Collections.singletonList(fakeMapEntry));
    assertEquals(1, chunkManager.getChunkMap().size());
    assertEquals(startTimePlusFourHours,
        chunkManager.getChunkMap().get(startTimePlusFourHours).info().startTimeSecs);

    chunkManager.removeStaleChunks(Collections.emptyList());
    assertEquals(1, chunkManager.getChunkMap().size());
    assertEquals(startTimePlusFourHours,
        chunkManager.getChunkMap().get(startTimePlusFourHours).info().startTimeSecs);
  }

  @Test(expected =  ReadOnlyChunkInsertionException.class)
  public void testReadOnlyChunkInsertion() throws ReadOnlyChunkInsertionException {
    assertTrue(chunkManager.getChunkMap().isEmpty());

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTime + 1, testValue));
    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimePlusTwoHours, testValue));
    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTimePlusFourHours, testValue));
    assertEquals(3, chunkManager.getChunkMap().size());

    chunkManager.toOffHeapChunkMap();

    assertEquals(3, chunkManager.getChunkMap().size());
    assertEquals(3, getReadOnlyChunkCount(chunkManager));

    chunkManager.addMetric(MetricUtils.makeMetricString(
        testMetricName, inputTagString1, startTime + 2, testValue));
  }
}
