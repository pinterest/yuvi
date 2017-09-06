package com.pinterest.yuvi.tagstore;

import static java.util.Collections.emptyList;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MetricTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testMetricCreation() {
    String metricName = "testMetric";
    Metric m = new Metric(metricName, emptyList());
    assertEquals(metricName, m.metricName);
    assertTrue(m.tags.isEmpty());
    assertEquals(metricName, m.fullMetricName);
  }

  @Test
  public void testMetricWithTagCreation() {
    String metricName = "testMetric";
    String tag1 = "k1=v1";
    ArrayList<String> tags = new ArrayList<>();
    tags.add(tag1);
    Metric m = new Metric(metricName, tags);
    assertEquals(metricName, m.metricName);
    assertEquals(1, m.tags.size());
    assertEquals(new Tag("k1", "v1", "k1=v1"), m.tags.get(0));
    assertEquals(Tag.parseTag(tag1), m.tags.get(0));
    assertEquals("testMetric k1=v1", m.fullMetricName);
  }

  @Test
  public void testMetricWithMultiTagCreation() {
    String metricName = "testMetric";
    String tag1 = "k1=v1";
    String tag2 = "k1=v2";
    String tag3 = " k2=v2";
    String tag4 = "k2=v1 ";
    String tag5 = "";

    List<String> tags = Arrays.asList(tag1, tag2, tag3, tag4, tag5, null);
    Metric m = new Metric(metricName, tags);
    assertEquals(metricName, m.metricName);
    assertEquals(4, m.tags.size());
    assertEquals(new Tag("k1", "v1", "k1=v1"), m.tags.get(0));
    assertEquals(new Tag("k1", "v2", "k1=v2"), m.tags.get(1));
    assertEquals(new Tag("k2", "v2", "k2=v2"), m.tags.get(2));
    assertEquals(new Tag("k2", "v1", "k2=v1"), m.tags.get(3));
    assertEquals("testMetric k1=v1 k1=v2 k2=v1 k2=v2", m.fullMetricName);
  }

  @Test
  public void testInvalidMetric() throws IllegalArgumentException {
    thrown.expect(IllegalArgumentException.class);
    new Metric("", emptyList());
  }

  @Test
  public void testNullMetricName() throws IllegalArgumentException {
    thrown.expect(IllegalArgumentException.class);
    new Metric(null, emptyList());
  }
}