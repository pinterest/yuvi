package com.pinterest.yuvi.metricandtagstore;

import static org.junit.Assert.assertThat;

import com.pinterest.yuvi.metricstore.VarBitMetricStore;
import com.pinterest.yuvi.models.Point;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.InvertedIndexTagStore;
import com.pinterest.yuvi.tagstore.Metric;
import com.pinterest.yuvi.tagstore.Query;

import junit.framework.TestCase;
import org.hamcrest.collection.IsIterableContainingInOrder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MetricsAndTagStoreImplTest extends TestCase {

  private MetricsAndTagStoreImpl ms;

  private final long ts = 100L;
  private final double value = 10;

  @Override
  public void setUp() {
    ms = new MetricsAndTagStoreImpl(new InvertedIndexTagStore(), new VarBitMetricStore());
  }

  public void testBasicsInYuviStore() {
    // query empty store.
    final String testMetricName1 = "testMetric1";
    final List<String> testTags1 = Arrays.asList("host=h1", "dc=dc1");
    assertTrue(ms.getSeries(new Query("test", Collections.emptyList())).isEmpty());

    // 1 metric 1 data point
    final Metric testMetric1 = new Metric(testMetricName1, testTags1);
    final String expectedMetricName1 = testMetricName1 + " dc=dc1 host=h1";
    final String queryTagString = " host=h1 dc=dc1";

    ms.addPoint(testMetric1, ts, value);

    assertTrue(ms.getSeries(Query.parse("test")).isEmpty());
    assertTrue(ms.getSeries(Query.parse("test host=h1")).isEmpty());
    assertTrue(ms.getSeries(Query.parse("test host=h1 dc=dc1")).isEmpty());

    final List<TimeSeries>
        series1 =
        ms.getSeries(Query.parse(testMetricName1 + queryTagString));
    assertEquals(1, series1.size());
    assertEquals(expectedMetricName1, series1.get(0).getMetric());
    assertEquals(1, series1.get(0).getPoints().size());
    assertThat(series1.get(0).getPoints(),
        IsIterableContainingInOrder.contains(new Point(ts, value)));

    // 1 metric 2 points
    ms.addPoint(testMetric1, ts * 2, value * 2);

    assertTrue(ms.getSeries(Query.parse("test")).isEmpty());
    assertTrue(ms.getSeries(Query.parse("test host=h1")).isEmpty());
    assertTrue(ms.getSeries(Query.parse("test host=h1 dc=dc1")).isEmpty());

    final List<TimeSeries>
        series2 =
        ms.getSeries(Query.parse(testMetricName1 + queryTagString));
    assertEquals(1, series2.size());
    assertEquals(expectedMetricName1, series2.get(0).getMetric());
    assertEquals(2, series2.get(0).getPoints().size());
    List<Point> expectedPoints2 =
        Arrays.asList(new Point(ts, value), new Point(ts * 2, value * 2));
    final TimeSeries timeseries12 = new TimeSeries(expectedMetricName1, expectedPoints2);
    assertThat(series2, IsIterableContainingInOrder.contains(timeseries12));

    // 2 metrics 2 points
    final String testMetricName2 = "testMetric2";
    final Metric testMetric2 = new Metric(testMetricName2, testTags1);
    final String expectedMetricName2 = testMetricName2 + " dc=dc1 host=h1";
    ms.addPoint(testMetric2, ts * 3, value * 3);

    assertTrue(ms.getSeries(Query.parse("test")).isEmpty());
    assertTrue(ms.getSeries(Query.parse("test host=h1")).isEmpty());
    assertTrue(ms.getSeries(Query.parse("test host=h1 dc=dc1")).isEmpty());

    final Point point21 = new Point(ts * 3, value * 3);
    assertThat(ms.getSeries(Query.parse(testMetricName2 + queryTagString)),
        IsIterableContainingInOrder.contains(new TimeSeries(expectedMetricName2,
            Collections.singletonList(point21))));
    assertThat(ms.getSeries(Query.parse(testMetricName1 + queryTagString)),
        IsIterableContainingInOrder.contains(timeseries12));

    // Add duplicate point to metric2
    ms.addPoint(testMetric2, ts * 3, value * 3);
    List<Point> expectedPoints4 = Arrays.asList(point21, point21);
    assertThat(ms.getSeries(Query.parse(testMetricName2 + queryTagString)),
        IsIterableContainingInOrder.contains(new TimeSeries(expectedMetricName2, expectedPoints4)));
    assertThat(ms.getSeries(Query.parse(testMetricName1 + queryTagString)),
        IsIterableContainingInOrder.contains(timeseries12));

    // Add third point to metric 2
    ms.addPoint(testMetric2, ts * 4, value * 4);
    List<Point> expectedPoints5 = Arrays.asList(point21, point21, new Point(ts * 4, value * 4));
    assertThat(ms.getSeries(Query.parse(testMetricName2 + queryTagString)),
        IsIterableContainingInOrder.contains((new TimeSeries(expectedMetricName2, expectedPoints5))));
    assertThat(ms.getSeries(Query.parse(testMetricName1 + queryTagString)),
        IsIterableContainingInOrder.contains(timeseries12));

    assertTrue(ms.getSeries(Query.parse("test")).isEmpty());
    assertTrue(ms.getSeries(Query.parse("test host=h1")).isEmpty());
    assertTrue(ms.getSeries(Query.parse("test host=h1 dc=dc1")).isEmpty());
  }


  public void testMultipleTimeSeriesResponse() {
    final String testMetricName1 = "testMetric1";
    final List<String> testTags1 = Arrays.asList("host=h1", "dc=dc1");
    final List<String> testTags2 = Arrays.asList("host=h2", "dc=dc1");
    assertTrue(ms.getSeries(new Query("test", Collections.emptyList())).isEmpty());

    final Metric testMetric1 = new Metric(testMetricName1, testTags1);
    final Metric testMetric2 = new Metric(testMetricName1, testTags2);

    ms.addPoint(testMetric1, ts, value);
    ms.addPoint(testMetric2, ts, value);
    Point p1 = new Point(ts, value);

    assertThat(ms.getSeries(Query.parse(testMetricName1 + " dc=dc1")),
        IsIterableContainingInOrder.contains(
            new TimeSeries(testMetricName1 + " dc=dc1 host=h1", Collections.singletonList(p1)),
            new TimeSeries(testMetricName1 + " dc=dc1 host=h2", Collections.singletonList(p1))));
  }

  // TODO: Query corrupt tag store and metric store.
}
