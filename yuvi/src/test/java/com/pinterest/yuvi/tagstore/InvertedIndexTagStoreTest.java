package com.pinterest.yuvi.tagstore;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

public class InvertedIndexTagStoreTest {

  public static final String METRIC1 = "metric.1";
  public static final String METRIC2 = "metric.2";
  public static final String METRIC3 = "metric.3";
  public static final String HOST_TAG = "host";

  InvertedIndexTagStore store;
  private List<Integer> ids;

  @Before
  public void setup() {
    store = new InvertedIndexTagStore();
    ids = new ArrayList<>();
  }

  @Test
  public void testDetailedCreate() {
    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v1"))));

    assertEquals(ids.get(0), store.get(new Metric(METRIC1, Collections.singletonList("k1=v1"))).get());
    assertEquals(store.getMetricNameFromId(ids.get(0)), "metric.1 k1=v1");
    assertEquals(4, store.getMetricIndex().size());
    assertEquals(1, store.lookupMetricIndex(METRIC1).getCardinality());
    assertEquals(1, store.lookupMetricIndex("k1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("k1=v1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("metric.1 k1=v1").getCardinality());

    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v2"))));
    assertEquals(ids.get(1), store.get(new Metric(METRIC1, Collections.singletonList("k1=v2"))).get());
    assertEquals(store.getMetricNameFromId(ids.get(1)), "metric.1 k1=v2");
    assertEquals(6, store.getMetricIndex().size());
    assertEquals(2, store.lookupMetricIndex(METRIC1).getCardinality());
    assertEquals(2, store.lookupMetricIndex("k1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("k1=v1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("k1=v2").getCardinality());
    assertEquals(1, store.lookupMetricIndex("metric.1 k1=v1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("metric.1 k1=v2").getCardinality());

    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k2=v1"))));
    assertEquals(ids.get(2), store.get(new Metric(METRIC1, Collections.singletonList("k2=v1"))).get());
    assertEquals(store.getMetricNameFromId(ids.get(2)), "metric.1 k2=v1");
    assertEquals(9, store.getMetricIndex().size());
    assertEquals(3, store.lookupMetricIndex(METRIC1).getCardinality());
    assertEquals(2, store.lookupMetricIndex("k1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("k2").getCardinality());
    assertEquals(1, store.lookupMetricIndex("k1=v1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("k1=v2").getCardinality());
    assertEquals(1, store.lookupMetricIndex("k2=v1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("metric.1 k1=v1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("metric.1 k1=v2").getCardinality());
    assertEquals(1, store.lookupMetricIndex("metric.1 k2=v1").getCardinality());

    ids.add(store.getOrCreate(new Metric(METRIC2, Arrays.asList("k2=v1"))));
    assertEquals(ids.get(3), store.get(new Metric(METRIC2, Arrays.asList("k2=v1"))).get());
    assertEquals(store.getMetricNameFromId(ids.get(3)), "metric.2 k2=v1");
    assertEquals(11, store.getMetricIndex().size());
    assertEquals(3, store.lookupMetricIndex(METRIC1).getCardinality());
    assertEquals(1, store.lookupMetricIndex(METRIC2).getCardinality());
    assertEquals(2, store.lookupMetricIndex("k1").getCardinality());
    assertEquals(2, store.lookupMetricIndex("k2").getCardinality());
    assertEquals(1, store.lookupMetricIndex("k1=v1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("k1=v2").getCardinality());
    assertEquals(1, store.lookupMetricIndex("k1=v1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("metric.1 k1=v1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("metric.1 k1=v2").getCardinality());
    assertEquals(1, store.lookupMetricIndex("metric.1 k2=v1").getCardinality());
    assertEquals(1, store.lookupMetricIndex("metric.2 k2=v1").getCardinality());
  }

  @Test
  public void testGet() {
    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Collections.singletonList("k1=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Collections.singletonList("k1=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Arrays.asList("k1=v1", "k2=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC3, emptyList())));

    assertEquals(6, ids.size());
    Set<Integer> deduped = new HashSet<>(ids);
    assertEquals(deduped.size(), ids.size());

    assertEquals(ids.get(0), store.get(new Metric(METRIC1, Collections.singletonList("k1=v1"))).get());
    assertEquals(ids.get(1), store.get(new Metric(METRIC2, Collections.singletonList("k1=v1"))).get());
    assertEquals(ids.get(2), store.get(new Metric(METRIC1, Collections.singletonList("k1=v2"))).get());
    assertEquals(ids.get(3), store.get(new Metric(METRIC2, Collections.singletonList("k1=v2"))).get());
    assertEquals(ids.get(4),
        store.get(new Metric(METRIC2, Arrays.asList("k1=v1", "k2=v1"))).get());
    assertEquals(ids.get(4),
        store.get(new Metric(METRIC2, Arrays.asList("k2=v1", "k1=v1"))).get());
    assertEquals(ids.get(5),
        store.get(new Metric(METRIC3, emptyList())).get());
  }

  @Test
  public void testDuplicateCreate() {
    List<Integer> ids = new ArrayList<>();
    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Collections.singletonList("k1=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Collections.singletonList("k1=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Arrays.asList("k1=v1", "k2=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC3, emptyList())));

    assertEquals(6, ids.size());
    Set<Integer> deduped = new HashSet<>(ids);
    assertEquals(deduped.size(), ids.size());

    assertEquals(ids.get(0).intValue(),
        store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v1"))));
    assertEquals(ids.get(1).intValue(),
        store.getOrCreate(new Metric(METRIC2, Collections.singletonList("k1=v1"))));
    assertEquals(ids.get(2).intValue(),
        store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v2"))));
    assertEquals(ids.get(3).intValue(),
        store.getOrCreate(new Metric(METRIC2, Collections.singletonList("k1=v2"))));
    assertEquals(ids.get(4).intValue(),
        store.getOrCreate(new Metric(METRIC2, Arrays.asList("k1=v1", "k2=v1"))));
    assertEquals(ids.get(4).intValue(),
        store.getOrCreate(new Metric(METRIC2, Arrays.asList("k2=v1", "k1=v1"))));
    assertEquals(ids.get(5).intValue(),
        store.getOrCreate(new Metric(METRIC3, emptyList())));
  }

  @Test
  public void testWildcardQuery() {
    ids.add(store.getOrCreate(new Metric(METRIC1, Arrays.asList("k1=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Arrays.asList("k1=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC1, Arrays.asList("k1=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Arrays.asList("k1=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Arrays.asList("k1=v1", "k2=v1"))));

    assertEquals(5, ids.size());
    Set<Integer> deduped = new HashSet<>(ids);
    assertEquals(deduped.size(), ids.size());

    assertEquals(Arrays.asList(ids.get(0), ids.get(2)), store.lookup(Query.parse("metric.1 k1=*")));
    assertEquals(Arrays.asList(ids.get(1), ids.get(3), ids.get(4)),
        store.lookup(Query.parse("metric.2 k1=*")));
    assertEquals(Collections.singletonList(ids.get(4)), store.lookup(Query.parse("metric.2 k2=*")));
    assertEquals(Collections.singletonList(ids.get(4)), store.lookup(Query.parse("metric.2 k1=v1 k2=*")));
    assertTrue(store.lookup(Query.parse("metric.1 k2=*")).isEmpty());
    assertTrue(store.lookup(Query.parse("metric.1 k2=*")).isEmpty());
    assertTrue(store.lookup(Query.parse("metric.2 k3=*")).isEmpty());
    assertTrue(store.lookup(Query.parse(METRIC3)).isEmpty());
  }

  @Test
  public void testStringTagEqualsValue() {
    ids.add(store.getOrCreate(new Metric(METRIC1, Arrays.asList("k1=v1", "k2=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC1, Arrays.asList("k1=v2", "k2=v1"))));

    assertEquals(2, ids.size());
    Set<Integer> deduped = new HashSet<>(ids);
    assertEquals(deduped.size(), ids.size());

    assertEquals(Collections.singletonList(ids.get(0)), query("metric.1 k1=v1"));
    assertEquals(Collections.singletonList(ids.get(0)), query("metric.1 k1=v1 k2=v2"));
    assertEquals(Collections.singletonList(ids.get(1)), query("metric.1 k1=v2"));
    assertEquals(Collections.singletonList(ids.get(1)), query("metric.1 k1=v2 k2=v1"));
  }

  @Test
  public void testExactQuery() {
    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Collections.singletonList("k1=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Collections.singletonList("k1=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Arrays.asList("k1=v1", "k2=v1"))));

    assertEquals(5, ids.size());
    Set<Integer> deduped = new HashSet<>(ids);
    assertEquals(deduped.size(), ids.size());

    assertEquals(Collections.singletonList(ids.get(0)), query("metric.1 k1=v1"));
    assertEquals(Arrays.asList(ids.get(1), ids.get(4)), query("metric.2 k1=v1"));
    assertEquals(Collections.singletonList(ids.get(2)), query("metric.1 k1=v2"));
    assertEquals(Collections.singletonList(ids.get(3)), query("metric.2 k1=v2"));
    assertEquals(Collections.singletonList(ids.get(4)), query("metric.2 k1=v1 k2=v1"));
    assertEquals(Collections.singletonList(ids.get(4)), query("metric.2 k2=v1 k1=v1"));
    assertEquals(Arrays.asList(ids.get(1), ids.get(4)), query("metric.2 k1=v1"));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)), query(METRIC1));
    assertEquals(Arrays.asList(ids.get(1), ids.get(3), ids.get(4)), query(METRIC2));

    assertTrue(query(METRIC3).isEmpty());
    assertTrue(query("metric.3 k1=v1").isEmpty());
    assertTrue(query("metric.3 k1=v3").isEmpty());
    assertTrue(query("metric.3 k3=v3").isEmpty());
    assertTrue(query("metric.3 k3=v1").isEmpty());

    assertTrue(query("metric.3 k1=v1 k2=v2").isEmpty());
    assertTrue(query("metric.3 k1=v3 k2=v2").isEmpty());
    assertTrue(query("metric.3 k3=v3 k2=v2").isEmpty());
    assertTrue(query("metric.3 k3=v1 k2=v2").isEmpty());

    assertTrue(query("metric.1 k3=v1").isEmpty());
    assertTrue(query("metric.1 k1=v3").isEmpty());
    assertTrue(query("metric.1 k3=v3").isEmpty());
    assertTrue(query("metric.1 k1=v1 k3=v3").isEmpty());
    assertTrue(query("metric.1 k1=v1 k3=v2").isEmpty());
    assertTrue(query("metric.1 k1=v1 k2=v2").isEmpty());

    assertTrue(query("metric=v2").isEmpty()); // valid since metric name is opaque.
    assertTrue(query("metric k1=v1k2=v2").isEmpty()); // valid since we parse on first =.
    assertTrue(query("metric.2 k2=v1k1=v1").isEmpty()); // valid since we parse on first =.
    assertTrue(query("metric.2 k1=v1k2=v1").isEmpty()); // valid since we parse on first =.

    // malformed inputs
    testMalformedInput(store, "metric k1-");
    testMalformedInput(store, "metric k1=");
    testMalformedInput(store, "metric k1=  k2=1");
    testMalformedInput(store, "metric k1=  k2=1");
    testMalformedInput(store, "metric =v");
  }

  @Test
  public void testRegExMatch() {
    assertTrue(query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-01.ops.ankh.morpork.com"))
        .isEmpty());  // exact match
    assertTrue(query(
        makeRegExQuery(METRIC1, HOST_TAG, ".*.ops.ankh.morpork.com"))
        .isEmpty());  // match postfix
    assertTrue(query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-01.ops.ankh.*")).isEmpty());  // exact prefix
    assertTrue(query(
        makeRegExQuery(METRIC1, HOST_TAG, ".*")).isEmpty());  // match anything
    assertTrue(query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-\\d(3).ops.ankh.morpork.com")).isEmpty());

    ids.add(store.getOrCreate(
        new Metric(METRIC1, Collections.singletonList("host=ogg-01.ops.ankh.morpork.com"))));

    assertEquals(1, ids.size());
    assertEquals(((Set<Integer>) new HashSet<>(ids)).size(), ids.size());

    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-01.ops.ankh.morpork.com")));  // exact match
    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeRegExQuery(METRIC1, HOST_TAG, ".*.ops.ankh.morpork.com")));  // match postfix
    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-01.ops.ankh.*")));  // exact prefix
    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeRegExQuery(METRIC1, HOST_TAG, ".*")));  // match anything
    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeRegExQuery(METRIC1, HOST_TAG,
            "ogg-01.ops.(ankh|quirm|tsort).morpork.com"))); // match grouping
    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-\\d+.ops.ankh.morpork.com"))); // match numbers

    assertTrue(query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-\\d(3).ops.ankh.morpork.com")).isEmpty());

    ids.add(store.getOrCreate(
        new Metric(METRIC1, Collections.singletonList("host=ogg-01.ops.ankh1.morpork.com"))));
    assertEquals(2, ids.size());

    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-01.ops.ankh.morpork.com")));
    assertEquals(Collections.singletonList(ids.get(1)), query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-01.ops.ankh1.morpork.com")));
    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-01.ops.(ankh|quirm|tsort).morpork.com")));
    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeRegExQuery(METRIC1, HOST_TAG, ".*.ops.ankh.morpork.com")));
    assertEquals(Arrays.asList(ids.get(0), ids.get(1)), query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-01.ops.ankh.*")));
    assertEquals(Arrays.asList(ids.get(0), ids.get(1)), query(
        makeRegExQuery(METRIC1, HOST_TAG, ".*")));
    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeRegExQuery(METRIC1, HOST_TAG,
            "ogg-01.ops.(ankh|quirm|tsort).morpork.com"))); // match grouping
    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-\\d+.ops.ankh.morpork.com"))); // match numbers

    assertTrue(query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-01.ops.ankh2.morpork.com")).isEmpty());
    assertTrue(query(
        makeRegExQuery(METRIC1, HOST_TAG, "ogg-\\d(3).ops.ankh.morpork.com")).isEmpty());
  }

  @Test(expected = PatternSyntaxException.class)
  public void testFailedRegExQuery() {
    ids.add(store.getOrCreate(
        new Metric(METRIC1, Collections.singletonList("host=ogg-01.ops.ankh.morpork.com"))));

    query(makeRegExQuery(METRIC1, HOST_TAG, "ogg-\\d(3.ops.ankh.morpork.com"));
  }

  private Query makeRegExQuery(String metricName, String tagKey, String regEx) {
    final List<TagMatcher> tagMatchers =
            Collections.singletonList(TagMatcher.regExMatch(tagKey, regEx));
    return new Query(metricName, tagMatchers);
  }

  @Test
  public void testOrQuery() {
    // Test empty store
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|", false, false)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|", true, false)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|", false, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|", true, true)).isEmpty());

    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("host=CMTDibbler"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Collections.singletonList("host=CMTDibbler"))));

    assertEquals(2, ids.size());
    Set<Integer> deduped = new HashSet<>(ids);
    assertEquals(deduped.size(), ids.size());

    // Test empty query
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|", false, false)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|", true, false)).isEmpty());

    List<Integer> id0 = Collections.singletonList(ids.get(0));
    // Test empty not queries: are equivalent to wildcard queries since they exclude nothing.
    assertEquals(id0, query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|", false, true)));
    assertEquals(id0, query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|", true, true)));

    // case insensitive literal or
    assertEquals(id0, query(makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler", true, false)));
    assertEquals(id0, query(makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|", true, false)));
    assertEquals(id0, query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|cmtdibbler|", true, false)));
    assertEquals(id0, query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|cmtdibbler", true, false)));
    assertEquals(id0, query(makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|cmtdibbler|Slant", true, false)));
    assertEquals(id0, query(makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|LutZe|Slant", true, false)));
    assertEquals(id0, query(makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|cmtdibbler", true, false)));
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|Leki", true, false)).isEmpty());

    // Test case sensitive not literal or expressions
    // case sensitive literal or
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler", false, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler|", false, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "|CMTDibbler|", false, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "|CMTDibbler", false, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|CMTDibbler|Slant", false, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler|LutZe|Slant", false, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|CMTDibbler", false, false)));
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|Leki",
        false, false)).isEmpty());

    // case insensitive literal or
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler", true, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|", true, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "|cmtdibbler|", true, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "|cmtdibbler", true, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|cmtdibbler|Slant", true, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|LutZe|Slant", true, false)));
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|cmtdibbler", true, false)));
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|Leki",
        true, false)).isEmpty());

    // Test case sensitive not literal or expressions
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler",
        false, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler|",
        false, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|CMTDibbler|",
        false, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|CMTDibbler|Slant",
        false, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler|LutZe|Slant",
        false, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|CMTDibbler",
        false, true)).isEmpty());
    assertEquals(id0, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|Leki", false, true)));

    // Test case insensitive not literal or expressions
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler",
        true, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|",
        true, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "|cmtdibbler|",
        true, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|cmtdibbler|Slant",
        true, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|LutZe|Slant",
        true, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|cmtdibbler",
        true, true)).isEmpty());

    // Add a new metric that is not excluded.
    ids.add(store.getOrCreate(new Metric(METRIC1, Arrays.asList("host=Bubbler"))));
    assertEquals(3, ids.size());

    // Test case sensitive not literal or expressions
    List<Integer> id2 = Collections.singletonList(ids.get(2));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler", false, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler|", false, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "|CMTDibbler|", false, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "|CMTDibbler", false, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|CMTDibbler|Slant", false, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler|LutZe|Slant", false, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|CMTDibbler", false, true)));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)), query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|Leki", false, true)));

    // case insensitive not literal or
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler", true, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|", true, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "|cmtdibbler|", true, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "|cmtdibbler", true, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|cmtdibbler|Slant", true, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|LutZe|Slant", true, true)));
    assertEquals(id2, query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|cmtdibbler", true, true)));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)), query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "LutZe|Slant|leki", true, true)));

    // Test multiple or matche expressions with multiple matches
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)), query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler|LutZe|Bubbler", false, false)));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)), query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|LutZe|bubbler", true, false)));
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler|LutZe|Bubbler",
        false, true)).isEmpty());
    assertTrue(query(makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|LutZe|bubbler",
        true, true)).isEmpty());

    // Test multiple or match expresssions with multiple matches.
    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("host=Ceki"))));
    assertEquals(4, ids.size());

    assertEquals(Arrays.asList(ids.get(0), ids.get(2)), query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler|LutZe|Bubbler", false, false)));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)), query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|LutZe|bubbler", true, false)));
    assertEquals(Collections.singletonList(ids.get(3)), query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "CMTDibbler|LutZe|Bubbler", false, true)));
    assertEquals(Collections.singletonList(ids.get(3)), query(
        makeLiteralOrQuery(METRIC1, HOST_TAG, "cmtdibbler|LutZe|bubbler", true, true)));
  }

  private Query makeLiteralOrQuery(String metricName, String tagKey, String queryString,
                                   boolean caseInsensitive, boolean excludeLiteral) {

    final List<TagMatcher> tagMatchers = Collections.singletonList(
        excludeLiteral ? TagMatcher.notLiteralOrMatch(tagKey, queryString, caseInsensitive)
            : TagMatcher.literalOrMatch(tagKey, queryString, caseInsensitive));
    return new Query(metricName, tagMatchers);
  }

  @Test
  public void testWilcardExpressions() {

    ids.add(store.getOrCreate(new Metric(METRIC1,
        Collections.singletonList("host=ogg-01.ops.ankh.morpork.com"))));
    ids.add(store.getOrCreate(new Metric(METRIC1,
        Collections.singletonList("fakehost=ogg-01.ops.ankh.morpork.com"))));
    ids.add(store.getOrCreate(new Metric(METRIC2,
        Collections.singletonList("host=ogg-01.ops.ankh.morpork.com"))));

    assertEquals(3, ids.size());
    assertEquals(((Set<Integer>) new HashSet<>(ids)).size(), ids.size());

    // Test basic wildcard and iwildcard queries
    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*", false)));
    assertEquals(Collections.singletonList(ids.get(1)), query(
        makeWildcardQuery(METRIC1, "fakehost", "*", false)));
    assertEquals(Collections.singletonList(ids.get(2)), query(
        makeWildcardQuery(METRIC2, HOST_TAG, "*", false)));
    assertEquals(Collections.singletonList(ids.get(0)), query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*", true)));
    assertEquals(Collections.singletonList(ids.get(1)), query(
        makeWildcardQuery(METRIC1, "fakehost", "*", true)));
    assertEquals(Collections.singletonList(ids.get(2)), query(
        makeWildcardQuery(METRIC2, HOST_TAG, "*", true)));
    assertTrue(query(
        makeWildcardQuery(METRIC1, "h", "*", true)).isEmpty());
    assertTrue(query(
        makeWildcardQuery(METRIC1, "h", "*", false)).isEmpty());
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*.morpork.org", false)).isEmpty()); // postfix fail
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*.MORPORK.org", true)).isEmpty());
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "magrat*", false)).isEmpty()); // prefix fail
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "MAGRAT*", true)).isEmpty());
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "magrat*com", false)).isEmpty()); // infix fail
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "MAGRAT*com", true)).isEmpty());
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*quirm*", false)).isEmpty());
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*QUIRM*", true)).isEmpty());

    // match simple
    List<Integer> id0 = Collections.singletonList(ids.get(0));
    assertEquals(id0, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*.morpork.com", false)));  // prefix
    assertEquals(id0, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "ogg*", false))); // postfix
    assertEquals(id0, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "ogg*com", false))); // infix
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*.MORPORK.com", false)).isEmpty());
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "OGG*", false)).isEmpty());
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "OGG*com", false)).isEmpty());

    assertEquals(id0, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*.MORPORK.com", true)));
    assertEquals(id0, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "OGG*", true)));
    assertEquals(id0, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "OGG*com", true)));
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*.MORPORK1.com", true)).isEmpty());
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "OGG1*", true)).isEmpty());
    assertTrue(query(
        makeWildcardQuery(METRIC1, HOST_TAG, "OGG1*com", true)).isEmpty());

    // Complicated wildcard queries
    matchComplexWildcardQueries(store, METRIC1, HOST_TAG, ids, true);
    matchComplexWildcardQueries(store, METRIC1, HOST_TAG, ids, false);

    // Query multiple metrics
    ids.add(store.getOrCreate(new Metric(METRIC1,
        Collections.singletonList("host=ogg-02.ops.ankh.morpork.com"))));
    ids.add(store.getOrCreate(new Metric(METRIC1,
        Collections.singletonList("host=ogg*mor"))));
    ids.add(store.getOrCreate(new Metric(METRIC1,
        Collections.singletonList("host=ogg*ops*"))));
    ids.add(store.getOrCreate(new Metric(METRIC1,
        Collections.singletonList("host=ogg*ops*mor"))));
    assertEquals(7, ids.size());
    assertEquals(((Set<Integer>) new HashSet<>(ids)).size(), ids.size());

    assertEquals(Collections.singletonList(ids.get(6)), query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*ops*mor", false)));
    assertEquals(Arrays.asList(ids.get(0), ids.get(3), ids.get(5), ids.get(6)), query(
        makeWildcardQuery(METRIC1, HOST_TAG, "ogg*ops*", false)));
    assertEquals(Arrays.asList(ids.get(0), ids.get(3), ids.get(6)), query(
        makeWildcardQuery(METRIC1, HOST_TAG, "ogg*ops*mor", false)));
    assertEquals(Collections.singletonList(ids.get(6)), query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*OPS*mor", true)));
    assertEquals(Arrays.asList(ids.get(0), ids.get(3), ids.get(5), ids.get(6)), query(
        makeWildcardQuery(METRIC1, HOST_TAG, "ogg*OPS*", true)));
    assertEquals(Arrays.asList(ids.get(0), ids.get(3), ids.get(6)), query(
        makeWildcardQuery(METRIC1, HOST_TAG, "ogg*ops*MOR", true)));
    assertEquals(Arrays.asList(ids.get(0), ids.get(3), ids.get(4), ids.get(5), ids.get(6)),
        query(
            makeWildcardQuery(METRIC1, HOST_TAG, "******", true)));

    // test wildcard with UTF8 strings. Make sure the file is UTF8 encoded for these tests to pass.
    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("host=Здравей'_хора"))));
    assertEquals(8, ids.size());
    List<Integer> id7 = Collections.singletonList(ids.get(7));
    assertEquals(id7, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "Здр*", false)));
    assertEquals(id7, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "Здр*", true)));
    assertEquals(id7, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*хора", false)));
    assertEquals(id7, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "*хора", true)));
    assertEquals(id7, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "Здр*ра", false)));
    assertEquals(id7, query(
        makeWildcardQuery(METRIC1, HOST_TAG, "Здр*ра", true)));
  }

  private void matchComplexWildcardQueries(InvertedIndexTagStore store, String metric1,
                                           String tagKey, List<Integer> ids,
                                           boolean caseInsensitive) {

    List<Integer> id = Collections.singletonList(ids.get(0));
    assertEquals(id, query(
        makeWildcardQuery(metric1, tagKey, "ogg*ops*com", caseInsensitive)));  // double infix
    assertEquals(id, query(
        makeWildcardQuery(metric1, tagKey, "ogg*ops*ank*com", caseInsensitive)));  // triple infix
    assertEquals(id, query(
        makeWildcardQuery(metric1, tagKey, "*morpork*", caseInsensitive)));  // prefix and postfix
    assertEquals(id, query(
        makeWildcardQuery(metric1, tagKey, "*ops*com", caseInsensitive)));  // infix and postfix
    assertEquals(id, query(makeWildcardQuery(metric1, tagKey, "*ops*mor*com",
        caseInsensitive)));  // double infix and postfix
    assertEquals(id, query(
        makeWildcardQuery(metric1, tagKey, "ogg*ops*", caseInsensitive)));  // pre and infix
    assertEquals(id, query(makeWildcardQuery(metric1, tagKey, "ogg*ops*mor*",
        caseInsensitive)));  // pre and double infix
    assertEquals(id, query(makeWildcardQuery(metric1, tagKey, "ogg***com",
        caseInsensitive)));  // pre and multi wildcard infix
    assertEquals(id, query(
        makeWildcardQuery(metric1, tagKey, "ogg*****", caseInsensitive)));  // multi wildcard prefix
    assertEquals(id, query(
        makeWildcardQuery(metric1, tagKey, "****com", caseInsensitive)));  // multi wildcard postfix
    assertEquals(id, query(makeWildcardQuery(metric1, tagKey, "****ogg*****mor****com****",
        caseInsensitive)));  // multi wildcards
  }

  private Query makeWildcardQuery(String metricName, String tagKey, String queryString,
                                  boolean caseInsensitive) {

    final List<TagMatcher> tagMatchers = Collections.singletonList(
        caseInsensitive ? TagMatcher.iwildcardMatch(tagKey, queryString)
            : TagMatcher.wildcardMatch(tagKey, queryString));
    return new Query(metricName, tagMatchers);
  }

  @Test
  public void testExtractTagValue() {
    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v1"))));

    assertThat(ImmutableMap.of(2, "v1"), is(store.getValuesForMetricKey(METRIC1, "k1")));
    assertTrue((store.getValuesForMetricKey(METRIC1, "k2").isEmpty()));

    ids.add(store.getOrCreate(new Metric(METRIC2, Collections.singletonList("k1=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC1, Collections.singletonList("k1=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Collections.singletonList("k1=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Arrays.asList("k1=v1", "k2=v1"))));

    assertEquals(5, ids.size());
    assertEquals(((Set<Integer>) new HashSet<>(ids)).size(), ids.size());

    assertThat(ImmutableMap.of(2, "v1", 4, "v2"),
        is(store.getValuesForMetricKey(METRIC1, "k1")));

    assertThat(ImmutableMap.of(3, "v1", 5, "v2", 6, "v1"),
        is(store.getValuesForMetricKey(METRIC2, "k1")));
    assertThat(ImmutableMap.of(6, "v1"),
        is(store.getValuesForMetricKey(METRIC2, "k2")));

    assertTrue((store.getValuesForMetricKey(METRIC1, "k2").isEmpty()));
    assertTrue((store.getValuesForMetricKey(METRIC2, "k3").isEmpty()));

    // Test metrics with multiple tags.
    String fakeKey = "k3=v3";
    ids.add(store.getOrCreate(new Metric(METRIC2, Arrays.asList("k1=v1", fakeKey))));
    ids.add(store.getOrCreate(new Metric(METRIC1, Arrays.asList("k1=v2", fakeKey))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Arrays.asList("k1=v2", fakeKey))));
    ids.add(store.getOrCreate(new Metric(METRIC2, Arrays.asList("k1=v1", "k2=v1", fakeKey))));

    assertEquals(9, ids.size());
    assertEquals(((Set<Integer>) new HashSet<>(ids)).size(), ids.size());

    assertThat(ImmutableMap.of(2, "v1", 4, "v2", 8, "v2"),
        is(store.getValuesForMetricKey(METRIC1, "k1")));
    assertThat(ImmutableMap.of(8, "v3"),
        is(store.getValuesForMetricKey(METRIC1, "k3")));

    assertThat(ImmutableMap.builder().put(3, "v1").put(5, "v2").put(6, "v1").put(7, "v1")
            .put(9, "v2").put(10, "v1").build(),
        is(store.getValuesForMetricKey(METRIC2, "k1")));
    assertThat(ImmutableMap.of(6, "v1", 10, "v1"),
        is(store.getValuesForMetricKey(METRIC2, "k2")));

    assertTrue((store.getValuesForMetricKey(METRIC1, "k2").isEmpty()));
    assertTrue((store.getValuesForMetricKey(METRIC2, "k5").isEmpty()));
  }

  private void testMalformedInput(InvertedIndexTagStore store, String query) {
    try {
      query(query);
      fail("Should fail with exception");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testMixedTagMatchers() {
    ids.add(store.getOrCreate(new Metric(METRIC1, Arrays.asList("k1=v1"))));
    ids.add(store.getOrCreate(new Metric(METRIC1, Arrays.asList("k1=v1", "k2=v2"))));
    ids.add(store.getOrCreate(new Metric(METRIC1, Arrays.asList("k1=v11"))));

    TagMatcher exactTagMatcher = new TagMatcher(MatchType.EXACT, new Tag("k1", "v1"));
    TagMatcher exactTagMatcher2 = new TagMatcher(MatchType.EXACT, new Tag("k2", "v2"));
    TagMatcher orMatcher = new TagMatcher(MatchType.LITERAL_OR, new Tag("k1", "v1|v11"));
    TagMatcher iOrMatcher = new TagMatcher(MatchType.ILITERAL_OR, new Tag("k1", "V1|V11"));
    TagMatcher orMatcher2 = new TagMatcher(MatchType.LITERAL_OR, new Tag("k2", "v2"));
    TagMatcher iOrMatcher2 = new TagMatcher(MatchType.ILITERAL_OR, new Tag("k2", "V2"));
    TagMatcher notOrMatcher = new TagMatcher(MatchType.NOT_LITERAL_OR, new Tag("k2", "v2"));
    TagMatcher notIOrMatcher = new TagMatcher(MatchType.NOT_ILITERAL_OR, new Tag("k2", "V2"));
    TagMatcher iWildcardMatcher = new TagMatcher(MatchType.IWILDCARD, new Tag("k1", "V*"));
    TagMatcher wildcardMatcher2 = new TagMatcher(MatchType.WILDCARD, new Tag("k2", "*"));
    TagMatcher iWildcardMatcher2 = new TagMatcher(MatchType.IWILDCARD, new Tag("k2", "V*"));
    TagMatcher regExpMatcher = new TagMatcher(MatchType.REGEXP, new Tag("k1", "(v|V).*"));
    TagMatcher regExpMatcher2 = new TagMatcher(MatchType.REGEXP, new Tag("k2", "(v|V).*"));

    List<Integer> id0 = Collections.singletonList(ids.get(0));
    List<Integer> id1 = Collections.singletonList(ids.get(1));
    assertEquals(Arrays.asList(ids.get(0), ids.get(1)),
        query(new Query(METRIC1, Collections.singletonList(exactTagMatcher))));
    assertEquals(id1, query(new Query(METRIC1, Arrays.asList(exactTagMatcher, exactTagMatcher2))));
    assertEquals(id1, query(new Query(METRIC1, Arrays.asList(exactTagMatcher, orMatcher2))));
    assertEquals(id1, query(new Query(METRIC1, Arrays.asList(exactTagMatcher, iOrMatcher2))));

    assertEquals(id0, query(new Query(METRIC1, Arrays.asList(exactTagMatcher, notOrMatcher))));
    assertEquals(id0, query(new Query(METRIC1, Arrays.asList(exactTagMatcher, notIOrMatcher))));

    assertEquals(Arrays.asList(ids.get(0), ids.get(2)),
        query(new Query(METRIC1, Arrays.asList(orMatcher, notIOrMatcher))));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)),
        query(new Query(METRIC1, Arrays.asList(orMatcher, notOrMatcher))));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)),
        query(new Query(METRIC1, Arrays.asList(iOrMatcher, notIOrMatcher))));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)),
        query(new Query(METRIC1, Arrays.asList(iOrMatcher, notOrMatcher))));

    assertEquals(id1, query(new Query(METRIC1, Arrays.asList(wildcardMatcher2, exactTagMatcher))));
    assertEquals(id1, query(new Query(METRIC1, Arrays.asList(iWildcardMatcher2, exactTagMatcher))));
    assertEquals(id1, query(new Query(METRIC1, Arrays.asList(iWildcardMatcher, orMatcher2))));
    assertEquals(id1, query(new Query(METRIC1, Arrays.asList(iWildcardMatcher, iOrMatcher2))));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)),
        query(new Query(METRIC1, Arrays.asList(iWildcardMatcher, notOrMatcher))));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)),
        query(new Query(METRIC1, Arrays.asList(iWildcardMatcher, notIOrMatcher))));

    assertEquals(id1, query(new Query(METRIC1, Arrays.asList(regExpMatcher2, exactTagMatcher))));
    assertEquals(id1, query(new Query(METRIC1, Arrays.asList(regExpMatcher2, exactTagMatcher))));
    assertEquals(id1, query(new Query(METRIC1, Arrays.asList(regExpMatcher2, orMatcher))));
    assertEquals(id1,
        query(new Query(METRIC1, Arrays.asList(regExpMatcher2, orMatcher))));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)),
        query(new Query(METRIC1, Arrays.asList(regExpMatcher, notOrMatcher))));
    assertEquals(Arrays.asList(ids.get(0), ids.get(2)),
        query(new Query(METRIC1, Arrays.asList(regExpMatcher, notIOrMatcher))));
  }

  private List<Integer> query(String queryString) {
    return query(Query.parse(queryString));
  }

  private List<Integer> query(Query q) {
    List<Integer> result = store.lookup(q);
    Collections.sort(result);
    return result;
  }
}
