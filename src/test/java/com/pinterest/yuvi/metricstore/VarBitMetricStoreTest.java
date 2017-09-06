package com.pinterest.yuvi.metricstore;

import com.pinterest.yuvi.models.Point;

import junit.framework.TestCase;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

public class VarBitMetricStoreTest extends TestCase {

  @Test
  public void testEmpty() {
    MetricStore store = new VarBitMetricStore();
    assertTrue(store.getSeries(1).isEmpty());
    assertTrue(store.getSeries(2).isEmpty());
  }

  @Test
  public void testSimpleInserts() {
    MetricStore store = new VarBitMetricStore();
    long uuid1 = 1;
    long uuid2 = 2;
    long ts = Instant.now().getEpochSecond();
    double value = 100;

    // 1 metric with 1 value.
    store.addPoint(uuid1, ts, value);
    List<Point> points = store.getSeries(uuid1);
    assertEquals(1, points.size());
    assertEquals(ts, points.get(0).getTs());
    assertEquals(value, points.get(0).getVal());

    // 1 metric with 2 values.
    store.addPoint(uuid1, ts + 1, value + 1);
    List<Point> points2 = store.getSeries(uuid1);
    assertEquals(2, points2.size());
    assertEquals(ts, points2.get(0).getTs());
    assertEquals(value, points2.get(0).getVal());
    assertEquals(ts + 1, points2.get(1).getTs());
    assertEquals(value + 1, points2.get(1).getVal());

    // 2 metrics with 2 values each.
    store.addPoint(uuid2, ts + 2, value + 2);
    List<Point> points31 = store.getSeries(uuid1);
    assertEquals(2, points31.size());
    assertEquals(ts, points31.get(0).getTs());
    assertEquals(value, points31.get(0).getVal());
    assertEquals(ts + 1, points31.get(1).getTs());
    assertEquals(value + 1, points31.get(1).getVal());
    List<Point> points32 = store.getSeries(uuid2);
    assertEquals(1, points32.size());
    assertEquals(ts + 2, points32.get(0).getTs());
    assertEquals(value + 2, points32.get(0).getVal());

    store.addPoint(uuid2, ts + 3, value + 3);
    List<Point> points41 = store.getSeries(uuid1);
    assertEquals(2, points41.size());
    assertEquals(ts, points41.get(0).getTs());
    assertEquals(value, points41.get(0).getVal());
    assertEquals(ts + 1, points41.get(1).getTs());
    assertEquals(value + 1, points41.get(1).getVal());
    List<Point> points42 = store.getSeries(uuid2);
    assertEquals(2, points42.size());
    assertEquals(ts + 2, points42.get(0).getTs());
    assertEquals(value + 2, points42.get(0).getVal());
    assertEquals(ts + 3, points42.get(1).getTs());
    assertEquals(value + 3, points42.get(1).getVal());
  }
}
