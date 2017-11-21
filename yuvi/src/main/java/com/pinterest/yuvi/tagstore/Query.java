package com.pinterest.yuvi.tagstore;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.Map.Entry;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A Query contains instructions to fetch a set of time series associated with a single metric
 * name. All time series have the given metric name and match the given set of tag matchers
 * returned by the query. Since the tag matcher query resolution is complex, we only allow one
 * tagMatcher per tagKey to keep the query resolution deterministic. This is an OpenTSDB limitation.
 */
public final class Query {

  public final String metricName;
  public final List<TagMatcher> tagMatchers;

  public Query(final String metricName, final List<TagMatcher> tagMatchers) {
    if (metricName == null || metricName.isEmpty() || tagMatchers == null) {
      throw new IllegalArgumentException("metric name or tag matcher can't be null.");
    }

    final Map<String, List<TagMatcher>> tagNameMap = tagMatchers.stream()
        .map(t -> new SimpleEntry<>(t.tag.key, t))
        .collect(groupingBy(Entry::getKey, mapping(Entry::getValue, toList())));

    tagNameMap.entrySet().forEach(tagKeyEntry -> {
      if (tagKeyEntry.getValue().size() != 1) {
        throw new IllegalArgumentException("Only one tagFilter is allowed per tagKey: "
            + tagKeyEntry.getKey() + " .But we found " + tagKeyEntry.getValue().toString());
      }
    });

    this.metricName = metricName;
    this.tagMatchers = tagMatchers;
  }

  /**
   * Parse a string into a query. This should only be used to make unit tests more concise.
   * Production code should take a more rigorous approach to query parsing
   * @param s a string of the form "metric.name dimension1=label1 dimension2=* ..."
   * @return a new Metric
   * @throws Exception if parsing failed
   */
  public static Query parse(String s) {
    List<String> splits = Arrays.asList(s.split(" "));
    String metricName = splits.get(0);
    List<TagMatcher> matchers = new ArrayList<>();
    for (String s2 : splits.subList(1, splits.size())) {
      Tag tag = Tag.parseTag(s2);
      if (tag.value.equals("*")) {
        matchers.add(TagMatcher.wildcardMatch(tag.key, "*"));
      } else {
        matchers.add(TagMatcher.exactMatch(tag));
      }
    }
    return new Query(metricName, matchers);
  }

  @Override
  public String toString() {
    return "Query{" +
        "metricName='" + metricName + " " +
        ", tagMatchers=" + tagMatchers +
        '}';
  }
}
