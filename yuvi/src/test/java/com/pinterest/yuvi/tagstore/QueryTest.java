package com.pinterest.yuvi.tagstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class QueryTest {
  private final String metric = "metric";
  private final TagMatcher tagMatcher1 = new TagMatcher(MatchType.EXACT, new Tag("test", "test"));

  @Test
  public void testQuery() {
    Query q = new Query(metric, Arrays.asList(tagMatcher1));
    assertEquals(metric, q.metricName);
    assertEquals(1, q.tagMatchers.size());
    assertEquals(tagMatcher1, q.tagMatchers.get(0));

    Query q1 = Query.parse(metric);
    assertEquals(metric, q1.metricName);
    assertTrue(q1.tagMatchers.isEmpty());

    Query q2 = Query.parse("metric k1=v1");
    assertEquals(metric, q2.metricName);
    assertEquals(1, q2.tagMatchers.size());
    assertEquals(new TagMatcher(MatchType.EXACT, new Tag("k1", "v1", "k1=v1")),
        q2.tagMatchers.get(0));

    Query q3 = Query.parse("metric k1=v1 k2=v2");
    assertEquals(metric, q3.metricName);
    assertEquals(2, q3.tagMatchers.size());
    assertEquals(new TagMatcher(MatchType.EXACT, new Tag("k1", "v1", "k1=v1")),
        q3.tagMatchers.get(0));
    assertEquals(new TagMatcher(MatchType.EXACT, new Tag("k2", "v2", "k2=v2")),
        q3.tagMatchers.get(1));

    Query q4 = Query.parse("metric k1=*");
    assertEquals(metric, q4.metricName);
    assertEquals(1, q4.tagMatchers.size());
    assertEquals(new TagMatcher(MatchType.WILDCARD, new Tag("k1", "*")),
        q4.tagMatchers.get(0));

    Query q5 = Query.parse("metric k1=* k2=v1");
    assertEquals(metric, q5.metricName);
    assertEquals(2, q5.tagMatchers.size());
    assertEquals(new TagMatcher(MatchType.WILDCARD, new Tag("k1", "*")),
        q5.tagMatchers.get(0));
    assertEquals(new TagMatcher(MatchType.EXACT, new Tag("k2", "v1", "k2=v1")),
        q5.tagMatchers.get(1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullMetricName() {
    new Query(null, Collections.emptyList());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyMetricName() {
    new Query(null, Collections.emptyList());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullTags() {
    new Query(metric, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicateTagKeysUsingConstructor() {
    new Query(metric, Arrays.asList(tagMatcher1, tagMatcher1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicateTagKeysUsingParse() {
    Query.parse("metric k1=v1 k1=v2");
  }
}
