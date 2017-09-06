package com.pinterest.yuvi.metricstore.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.pinterest.yuvi.metricstore.MetricStore;
import com.pinterest.yuvi.metricstore.VarBitMetricStore;
import com.pinterest.yuvi.metricstore.VarBitTimeSeries;
import com.pinterest.yuvi.models.Point;

import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class OffHeapVarBitMetricStoreTest {
  final static double delta = 0.00001;
  final String testFileName = "";

  @Test
  public void testEmpty() {
    MetricStore store = new OffHeapVarBitMetricStore(1, testFileName);
    assertTrue(store.getSeries(1).isEmpty());
    assertTrue(store.getSeries(2).isEmpty());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testReadOnlyStore() {
    MetricStore store = new OffHeapVarBitMetricStore(1, testFileName);
    store.addPoint(1, 1, 1);
  }

  @Test
  public void testSimpleInserts() {
    MetricStore heapStore = new VarBitMetricStore();
    long uuid1 = 1;
    long uuid2 = 2;
    long ts = Instant.now().getEpochSecond();
    double value = 100;

    // 1 metric with 1 value.
    heapStore.addPoint(uuid1, ts, value);
    OffHeapVarBitMetricStore offheapStore1 =
        OffHeapVarBitMetricStore.toOffHeapStore(getSeriesMap(heapStore), testFileName);

    assertEquals(1, offheapStore1.getSeriesMap().size());
    List<Point> points = offheapStore1.getSeries(uuid1);
    assertEquals(1, points.size());
    assertEquals(ts, points.get(0).getTs());
    assertEquals(value, points.get(0).getVal(), delta);

    // 1 metric with 2 values.
    heapStore.addPoint(uuid1, ts + 1, value + 1);
    OffHeapVarBitMetricStore offheapStore2 =
        OffHeapVarBitMetricStore.toOffHeapStore(getSeriesMap(heapStore), testFileName);
    List<Point> points2 = offheapStore2.getSeries(uuid1);
    assertEquals(2, points2.size());
    assertEquals(ts, points2.get(0).getTs());
    assertEquals(value, points2.get(0).getVal(), delta);
    assertEquals(ts + 1, points2.get(1).getTs());
    assertEquals(value + 1, points2.get(1).getVal(), delta);

    // 2 metrics with 2 values each.
    heapStore.addPoint(uuid2, ts + 2, value + 2);
    OffHeapVarBitMetricStore offheapStore3 =
        OffHeapVarBitMetricStore.toOffHeapStore(getSeriesMap(heapStore), testFileName);
    List<Point> points31 = offheapStore3.getSeries(uuid1);
    assertEquals(2, points31.size());
    assertEquals(ts, points31.get(0).getTs());
    assertEquals(value, points31.get(0).getVal(), delta);
    assertEquals(ts + 1, points31.get(1).getTs());
    assertEquals(value + 1, points31.get(1).getVal(), delta);
    List<Point> points32 = offheapStore3.getSeries(uuid2);
    assertEquals(1, points32.size());
    assertEquals(ts + 2, points32.get(0).getTs());
    assertEquals(value + 2, points32.get(0).getVal(), delta);

    heapStore.addPoint(uuid2, ts + 3, value + 3);
    OffHeapVarBitMetricStore offheapStore4 =
        OffHeapVarBitMetricStore.toOffHeapStore(getSeriesMap(heapStore), testFileName);
    List<Point> points41 = offheapStore4.getSeries(uuid1);
    assertEquals(2, points41.size());
    assertEquals(ts, points41.get(0).getTs());
    assertEquals(value, points41.get(0).getVal(), delta);
    assertEquals(ts + 1, points41.get(1).getTs());
    assertEquals(value + 1, points41.get(1).getVal(), delta);
    List<Point> points42 = offheapStore4.getSeries(uuid2);
    assertEquals(2, points42.size());
    assertEquals(ts + 2, points42.get(0).getTs());
    assertEquals(value + 2, points42.get(0).getVal(), delta);
    assertEquals(ts + 3, points42.get(1).getTs());
    assertEquals(value + 3, points42.get(1).getVal(), delta);
  }

  @SuppressWarnings("unchecked")
  private Map<Long, VarBitTimeSeries> getSeriesMap(MetricStore heapStore) {
    return (Map<Long, VarBitTimeSeries>) heapStore.getSeriesMap();
  }
}
