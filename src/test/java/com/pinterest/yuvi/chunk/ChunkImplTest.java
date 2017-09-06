package com.pinterest.yuvi.chunk;

import static com.pinterest.yuvi.chunk.MetricUtils.parseAndAddOpenTSDBMetric;
import static com.pinterest.yuvi.chunk.MetricUtils.makeMetricString;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.pinterest.yuvi.metricandtagstore.MetricsAndTagStoreImpl;
import com.pinterest.yuvi.metricstore.VarBitMetricStore;
import com.pinterest.yuvi.models.Point;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.InvertedIndexTagStore;
import com.pinterest.yuvi.tagstore.Query;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ChunkImplTest {

  private final long startTime = 1488499200;  // Fri, 03 Mar 2017 00:00:00 UTC
  private final long endTime = 1488499200 + 3600 * 2;

  private final long testTs = 100L;
  private final double testValue = 10;

  private Chunk chunk;

  @Before
  public void setUp() {
    chunk = new ChunkImpl(
        new MetricsAndTagStoreImpl(new InvertedIndexTagStore(20), new VarBitMetricStore()),
        new ChunkInfo("test", startTime, endTime));
  }

  @Test
  public void testChunkContainsData() {
    assertTrue(chunk.containsDataInTimeRange(startTime - 1, endTime + 1));
    assertTrue(chunk.containsDataInTimeRange(startTime + 1, endTime + 1));
    assertTrue(chunk.containsDataInTimeRange(startTime + 1, endTime - 1));
    assertTrue(chunk.containsDataInTimeRange(startTime - 1, endTime - 1));

    assertTrue(chunk.containsDataInTimeRange(startTime, endTime));
    assertTrue(chunk.containsDataInTimeRange(startTime, endTime - 1));
    assertTrue(chunk.containsDataInTimeRange(startTime, endTime + 1));
    assertTrue(chunk.containsDataInTimeRange(startTime + 1, endTime));
    assertTrue(chunk.containsDataInTimeRange(startTime - 1, endTime));

    assertFalse(chunk.containsDataInTimeRange(startTime - 10000, endTime - 10000));
    assertFalse(chunk.containsDataInTimeRange(startTime + 10000, endTime + 10000));
  }

  @Test
  public void testChunkIngestion() {
    final String testMetricName1 = "testMetric1";

    // 1 metric 1 data point
    final String expectedMetricName1 = testMetricName1 + " dc=dc1 host=h1";
    final String queryTagString = " host=h1 dc=dc1";

    String inputTagString = "host=h1 dc=dc1";
    parseAndAddOpenTSDBMetric(makeMetricString(testMetricName1, inputTagString, testTs, testValue),
        chunk);

    assertTrue(chunk.query(Query.parse("test")).isEmpty());
    assertTrue(chunk.query(Query.parse("test host=h1")).isEmpty());
    assertTrue(chunk.query(Query.parse("test host=h1 dc=dc1")).isEmpty());

    final List<TimeSeries> series1 = chunk.query(Query.parse(testMetricName1 + queryTagString));
    assertEquals(1, series1.size());
    assertEquals(expectedMetricName1, series1.get(0).getMetric());
    assertEquals(1, series1.get(0).getPoints().size());
    List<Point> expectedPoints1 = Arrays.asList(new Point(testTs, testValue));
    Assert.assertThat(series1.get(0).getPoints(),
        IsIterableContainingInOrder.contains(expectedPoints1.toArray()));

    // 1 metric 2 points
    parseAndAddOpenTSDBMetric(
        makeMetricString(testMetricName1, inputTagString, testTs * 2, testValue * 2), chunk);

    assertTrue(chunk.query(Query.parse("test")).isEmpty());
    assertTrue(chunk.query(Query.parse("test host=h1")).isEmpty());
    assertTrue(chunk.query(Query.parse("test host=h1 dc=dc1")).isEmpty());

    final List<TimeSeries> series2 = chunk.query(Query.parse(testMetricName1 + queryTagString));
    assertEquals(1, series2.size());
    assertEquals(expectedMetricName1, series2.get(0).getMetric());
    assertEquals(2, series2.get(0).getPoints().size());
    List<Point> expectedPoints2 =
        Arrays.asList(new Point(testTs, testValue), new Point(testTs * 2, testValue * 2));
    final Object[] timeseries12 = Arrays.asList(
        new TimeSeries(expectedMetricName1, expectedPoints2)).toArray();
    Assert.assertThat(series2, IsIterableContainingInOrder.contains(timeseries12));

    // 2 metrics 2 points
    final String testMetricName2 = "testMetric2";
    final String expectedMetricName2 = testMetricName2 + " dc=dc1 host=h1";
    parseAndAddOpenTSDBMetric(
        makeMetricString(testMetricName2, inputTagString, testTs * 3, testValue * 3), chunk);

    assertTrue(chunk.query(Query.parse("test")).isEmpty());
    assertTrue(chunk.query(Query.parse("test host=h1")).isEmpty());
    assertTrue(chunk.query(Query.parse("test host=h1 dc=dc1")).isEmpty());

    final Point point21 = new Point(testTs * 3, testValue * 3);
    List<Point> expectedPoints3 = Arrays.asList(point21);
    Assert.assertThat(chunk.query(Query.parse(testMetricName2 + queryTagString)),
        IsIterableContainingInOrder.contains(
            Arrays.asList(new TimeSeries(expectedMetricName2, expectedPoints3)).toArray()));
    Assert.assertThat(chunk.query(Query.parse(testMetricName1 + queryTagString)),
        IsIterableContainingInOrder.contains(timeseries12));

    // Add duplicate point to metric2
    parseAndAddOpenTSDBMetric(
        makeMetricString(testMetricName2, inputTagString, testTs * 3, testValue * 3), chunk);
    List<Point> expectedPoints4 = Arrays.asList(point21, point21);
    Assert.assertThat(chunk.query(Query.parse(testMetricName2 + queryTagString)),
        IsIterableContainingInOrder.contains(
            Arrays.asList(new TimeSeries(expectedMetricName2, expectedPoints4)).toArray()));
    Assert.assertThat(chunk.query(Query.parse(testMetricName1 + queryTagString)),
        IsIterableContainingInOrder.contains(timeseries12));

    // Add third point to metric 2
    parseAndAddOpenTSDBMetric(
        makeMetricString(testMetricName2, inputTagString, testTs * 4, testValue * 4), chunk);
    List<Point>
        expectedPoints5 =
        Arrays.asList(point21, point21, new Point(testTs * 4, testValue * 4));
    Assert.assertThat(chunk.query(Query.parse(testMetricName2 + queryTagString)),
        IsIterableContainingInOrder.contains(
            Arrays.asList(new TimeSeries(expectedMetricName2, expectedPoints5)).toArray()));
    Assert.assertThat(chunk.query(Query.parse(testMetricName1 + queryTagString)),
        IsIterableContainingInOrder.contains(timeseries12));

    assertTrue(chunk.query(Query.parse("test")).isEmpty());
    assertTrue(chunk.query(Query.parse("test host=h1")).isEmpty());
    assertTrue(chunk.query(Query.parse("test host=h1 dc=dc1")).isEmpty());
  }

  @Test
  public void testMultipleTimeSeriesResponse() {
    setUp();
    final String testMetricName1 = "testMetric1";
    final List<String> testTags1 = Arrays.asList("host=h1", "dc=dc1");
    final List<String> testTags2 = Arrays.asList("host=h2", "dc=dc1");
    assertTrue(chunk.query(new Query("test", Collections.emptyList())).isEmpty());

    parseAndAddOpenTSDBMetric(
        makeMetricString(testMetricName1, "host=h1 dc=dc1", testTs, testValue), chunk);
    parseAndAddOpenTSDBMetric(
        makeMetricString(testMetricName1, "host=h2 dc=dc1", testTs, testValue), chunk);

    Point p1 = new Point(testTs, testValue);

    final Object[] expectedTimeSeries = Arrays.asList(
        new TimeSeries(testMetricName1 + " dc=dc1 host=h1", Arrays.asList(p1)),
        new TimeSeries(testMetricName1 + " dc=dc1 host=h2", Arrays.asList(p1))
    ).toArray();
    Assert.assertThat(chunk.query(Query.parse(testMetricName1 + " dc=dc1")),
        IsIterableContainingInOrder.contains(expectedTimeSeries));
  }
}
