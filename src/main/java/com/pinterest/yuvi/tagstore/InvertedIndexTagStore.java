package com.pinterest.yuvi.tagstore;

import com.google.common.annotations.VisibleForTesting;
import net.openhft.chronicle.map.ChronicleMap;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This class implements the tag store interface using an inverted index. The inverted index will
 * be implemented using a roaring bitmap.
 *
 * This tag store will contain 2 maps:
 * a) One map maps a string to a List[int]
 * b) Other map maps a int to a string.
 */
public class InvertedIndexTagStore implements TagStore {

  private static final String MISSING_METRIC = "";

  private static final RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();

  private static final int DEFAULT_UUID_MAP_SIZE = 1000;

  private static final int AVERAGE_METRIC_NAME_SIZE = 1024;

  private Map<String, RoaringBitmap> nameMap;
  private Map<Integer, String> uuidMap;

  private AtomicInteger tagStoreCounter;

  public InvertedIndexTagStore() {
    this(DEFAULT_UUID_MAP_SIZE);
  }

  public InvertedIndexTagStore(int initialMapSize) {
    this(new ConcurrentHashMap<>(), ChronicleMap.of(Integer.class, String.class)
        .entries(initialMapSize)
        .averageValueSize(AVERAGE_METRIC_NAME_SIZE)
        .create());
  }

  public InvertedIndexTagStore(Map<String, RoaringBitmap> nameMap, Map<Integer, String> uuidMap) {
    this.tagStoreCounter = new AtomicInteger(1);
    this.nameMap = nameMap;
    this.uuidMap = uuidMap;
  }

  @Override
  public Optional<Integer> get(Metric m) {
    if (nameMap.containsKey(m.fullMetricName)) {
      return Optional.of(nameMap.get(m.fullMetricName).getIntIterator().next());
    }
    return Optional.empty();
  }

  @Override
  public List<Integer> lookup(Query q) {
    PeekableIntIterator ids = lookupIds(q).getIntIterator();
    return getIntegers(ids);
  }

  private List<Integer> getIntegers(PeekableIntIterator ids) {
    if (ids.hasNext()) {
      List<Integer> result = new ArrayList<>();
      while (ids.hasNext()) {
        result.add(ids.next());
      }
      return result;
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * This function fetches the ids in tag store that match the given query. In the current OpenTSDB
   * use case, a typical query consists of a metric name and a set of tag matchers. Currently, we
   * can have only one tagMatcher per tag key and a tag matcher is not reused, so we inline the tag
   * key and value parsing logic here. If tag matchers in slow path are used among queries, it is
   * worth creating a specialized tag matcher class for each match type.
   */
  private RoaringBitmap lookupIds(final Query q) {
    Predicate<TagMatcher> notTagMatcher =
        t -> (t.type == MatchType.NOT_LITERAL_OR || t.type == MatchType.NOT_ILITERAL_OR);

    List<TagMatcher> inclusiveTagMatchers =
        q.tagMatchers.stream().filter(notTagMatcher.negate()).collect(Collectors.toList());

    List<TagMatcher> exclusionTagMatchers =
        q.tagMatchers.stream().filter(notTagMatcher).collect(Collectors.toList());

    // If no tag filters are specifed, then just match on metric name. If no exclusion tag filters
    // are given just match on the included tag filters.
    if ((inclusiveTagMatchers.isEmpty() && exclusionTagMatchers.isEmpty())
        || (!inclusiveTagMatchers.isEmpty() && exclusionTagMatchers.isEmpty())) {

      return getIncludedIds(q.metricName, inclusiveTagMatchers);
    }

    // If only exclusion tag matchers are specified, get all the metrics that match the wildcard
    // tag and then remove the omit the ones that match the OR query.
    // TODO: This case can be slightly faster if there is a function that computes negations.
    if (inclusiveTagMatchers.isEmpty() && !exclusionTagMatchers.isEmpty()) {
      inclusiveTagMatchers = exclusionTagMatchers.stream()
          .map(matcher -> TagMatcher.wildcardMatch(matcher.tag.key, "*"))
          .collect(Collectors.toList());
    }

    // If both included and excluded tag matchers are given, do a difference operation.
    RoaringBitmap inclusiveIds = getIncludedIds(q.metricName, inclusiveTagMatchers);
    RoaringBitmap excludedIds = getExcludedIds(q.metricName, exclusionTagMatchers);
    return RoaringBitmap.andNot(inclusiveIds, excludedIds);
  }

  private RoaringBitmap getIncludedIds(String metricName, List<TagMatcher> tagMatchers) {
    List<RoaringBitmap> andBitMaps = new ArrayList<>();

    if (nameMap.containsKey(metricName)) {
      andBitMaps.add(nameMap.get(metricName));
    } else {
      andBitMaps.add(EMPTY_BITMAP); // If no metric name is present, return empty bitmap
    }

    for (TagMatcher t : tagMatchers) {
      if (nameMap.containsKey(t.tag.key)) {
        // Include all metrics that include this tag key.
        andBitMaps.add(nameMap.get(t.tag.key));

        // Fast path looks up inverted index.

        // host=* type wildcard queries can also resolved in fast path.
        // Since the metrics for the key are already added, there is no more work to do.
        if ((t.type.equals(MatchType.IWILDCARD) || t.type.equals(MatchType.WILDCARD))
            && t.tag.value.equals("*")) {
          continue;
        }

        if (t.type.equals(MatchType.EXACT)) {
          matchExactTag(t, andBitMaps);
          continue;
        }

        if (t.type.equals(MatchType.LITERAL_OR)) {
          matchOr(t, andBitMaps);
          continue;
        }

        // Slow path gets the values for the metric and then matches based on the logic.
        Map<Integer, String> valuesWithMetricIds = getValuesForMetricKey(metricName, t.tag.key);

        // wildcard and iwildcard
        if (t.type.equals(MatchType.IWILDCARD) || t.type.equals(MatchType.WILDCARD)) {
          matchWildcard(t, valuesWithMetricIds, andBitMaps);
          continue;
        }

        if (t.type.equals(MatchType.ILITERAL_OR)) {
          matchCaseInsensitiveOr(t, valuesWithMetricIds, andBitMaps);
          continue;
        }

        if (t.type.equals(MatchType.REGEXP)) {
          matchRegEx(t, valuesWithMetricIds, andBitMaps);
          continue;
        }
      } else {
        andBitMaps.add(EMPTY_BITMAP);
      }
    }
    RoaringBitmap result = FastAggregation.and(andBitMaps.iterator());
    return result;
  }

  private void matchWildcard(TagMatcher t, Map<Integer, String> valuesWithMetricIds,
                             List<RoaringBitmap> resultMap) {

    final boolean caseInsensitive = t.type.equals(MatchType.IWILDCARD);
    String actual = caseInsensitive ? t.tag.value.toLowerCase() : t.tag.value;

    // Parse wild card string
    boolean hasPostfix;
    boolean hasPrefix;
    String[] components;

    if (actual.charAt(0) == '*') {
      hasPostfix = true;
      while (actual.charAt(0) == '*') {
        if (actual.length() < 2) {
          break;
        }
        actual = actual.substring(1);
      }
    } else {
      hasPostfix = false;
    }

    if (actual.charAt(actual.length() - 1) == '*') {
      hasPrefix = true;
      while (actual.charAt(actual.length() - 1) == '*') {
        if (actual.length() < 2) {
          break;
        }
        actual = actual.substring(0, actual.length() - 1);
      }
    } else {
      hasPrefix = false;
    }
    if (actual.indexOf('*') > 0) {
      components = actual.split("\\*");
    } else {
      components = new String[1];
      components[0] = actual;
    }

    // Match the wildcard string
    RoaringBitmap wildcardMatches = new RoaringBitmap();
    valuesWithMetricIds.entrySet().stream()
        .filter(entry -> matchTagsWithWildcard(hasPostfix, hasPrefix, components,
            caseInsensitive ? entry.getValue().toLowerCase() : entry.getValue()))
        .forEach(entry -> wildcardMatches.add(entry.getKey()));

    if (wildcardMatches.getCardinality() > 0) {
      resultMap.add(wildcardMatches);
    } else {
      resultMap.add(EMPTY_BITMAP);
    }
  }

  private boolean matchTagsWithWildcard(boolean hasPostfix, boolean hasPrefix, String[] components,
                                        String tagValue) {
    if (tagValue == null) {
      return false;
    }

    if (components.length == 1 && components[0].equals("*")) {  // match all
      return true;
    }

    if (hasPostfix && !hasPrefix && !tagValue.endsWith(components[components.length - 1])) {
      return false;
    }

    if (hasPrefix && !hasPostfix && !tagValue.startsWith(components[0])) {
      return false;
    }

    int idx = 0;
    for (int i = 0; i < components.length; i++) {
      if (tagValue.indexOf(components[i], idx) < 0) {
        return false;
      }
      idx += components[i].length();
    }

    return true;
  }

  private RoaringBitmap getExcludedIds(String metricName, List<TagMatcher> tagMatchers) {
    List<RoaringBitmap> andNotBitMaps = new ArrayList<>();

    if (nameMap.containsKey(metricName)) {
      andNotBitMaps.add(nameMap.get(metricName));
    } else {
      andNotBitMaps.add(EMPTY_BITMAP);
    }

    for (TagMatcher t : tagMatchers) {
      if (nameMap.containsKey(t.tag.key)) {
        // Include all metrics that include this tag key.
        andNotBitMaps.add(nameMap.get(t.tag.key));

        // Slow path gets the values for the metric and then matches based on the logic.
        Map<Integer, String> valuesWithMetricIds = getValuesForMetricKey(metricName, t.tag.key);

        if (t.type.equals(MatchType.NOT_LITERAL_OR)) {
          matchOr(t, andNotBitMaps);
          continue;
        }

        if (t.type.equals(MatchType.NOT_ILITERAL_OR)) {
          matchCaseInsensitiveOr(t, valuesWithMetricIds, andNotBitMaps);
          continue;
        }
      } else {
        andNotBitMaps.add(EMPTY_BITMAP);
      }
    }
    return FastAggregation.and(andNotBitMaps.iterator());
  }

  private void matchExactTag(TagMatcher t, List<RoaringBitmap> resultMap) {
    String rawTag = t.tag.key + "=" + t.tag.value;
    if (nameMap.containsKey(rawTag)) {
      resultMap.add(nameMap.get(rawTag));
    } else {
      resultMap.add(EMPTY_BITMAP);  // If not exact match is present, return empty bitmap.
    }
  }

  private void matchRegEx(TagMatcher t, Map<Integer, String> valuesWithMetricIds,
                          List<RoaringBitmap> resultMap) {
    Pattern pattern = Pattern.compile(t.tag.value);
    RoaringBitmap regExpMatches = new RoaringBitmap();
    valuesWithMetricIds.entrySet().stream()
        .filter(entry -> pattern.matcher(entry.getValue()).find())
        .forEach(entry -> regExpMatches.add(entry.getKey()));
    if (regExpMatches.getCardinality() > 0) {
      resultMap.add(regExpMatches);
    } else {
      resultMap.add(EMPTY_BITMAP);
    }
  }

  private void matchOr(TagMatcher t, List<RoaringBitmap> resultantMap) {
    // TODO: Add more validation for this expression here.
    final String[] split = t.tag.value.split("\\|");
    List<RoaringBitmap> orMaps = new ArrayList<>();
    for (int i = 0; i < split.length; i++) {
      if (!split[i].isEmpty()) {
        final String tagValuePair = t.tag.key + "=" + split[i];
        if (nameMap.containsKey(tagValuePair)) {
          orMaps.add(nameMap.get(tagValuePair));
        }
      }
    }

    if (orMaps.isEmpty()) {
      resultantMap.add(EMPTY_BITMAP); // If no exact match is present, return empty result.
    } else {
      resultantMap.add(FastAggregation.or(orMaps.iterator()));
    }
  }

  private void matchCaseInsensitiveOr(TagMatcher t, Map<Integer, String> valuesWithMetricIds,
                                      List<RoaringBitmap> resultantMap) {

    RoaringBitmap iLiteralMatches = new RoaringBitmap();
    final String[] split = t.tag.value.split("\\|");

    if (split.length > 0) {
      valuesWithMetricIds.entrySet().stream()
          .filter(entry -> caseInsensitiveTagValueMatch(split, entry.getValue()))
          .forEach(entry -> iLiteralMatches.add(entry.getKey()));
    }

    if (iLiteralMatches.getCardinality() > 0) {
      resultantMap.add(iLiteralMatches);
    } else {
      resultantMap.add(EMPTY_BITMAP);
    }
  }

  private boolean caseInsensitiveTagValueMatch(String[] tags, String tagValue) {
    for (int i = 0; i < tags.length; i++) {
      if (!tags[i].isEmpty() && !tagValue.isEmpty()
          && tags[i].toLowerCase().equals(tagValue.toLowerCase())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Given a metric name, extract all the values associated for the given key.
   */
  @VisibleForTesting
  public Map<Integer, String> getValuesForMetricKey(String metricName, String key) {
    List<RoaringBitmap> andBitMaps = new ArrayList<>();

    if (nameMap.containsKey(metricName)) {
      andBitMaps.add(nameMap.get(metricName));
    } else {
      andBitMaps.add(EMPTY_BITMAP);
    }

    if (nameMap.containsKey(key)) {
      andBitMaps.add(nameMap.get(key));
    } else {
      andBitMaps.add(EMPTY_BITMAP);
    }

    RoaringBitmap resultBitMap = FastAggregation.and(andBitMaps.iterator());
    HashMap<Integer, String> resultMap = new HashMap<>();
    for (int i : resultBitMap.toArray()) {
      resultMap.put(i, extractTagValueForTagKey(uuidMap.get(i), key));
    }
    return resultMap;
  }

  /**
   * Given a full metric string, extract the values for a given key. Currently, we only assume that
   * a tagKey exists only once in metric name. Currently, OpenTSDB doesn't support repetitions of
   * the tag key in a metric. So, we only extract the first value in this method.
   */
  private String extractTagValueForTagKey(String fullMetric, String key) {
    int startingIndex = fullMetric.indexOf(key + "=");
    if (startingIndex == -1) {
      throw new IllegalArgumentException(
          "Key: " + key + "should exist in metric name " + fullMetric);
    }
    int startValueIndex = startingIndex + key.length() + 1;
    int endValueIndex = fullMetric.indexOf(" ", startValueIndex); // first space is end.
    return fullMetric
        .substring(startValueIndex, endValueIndex == -1 ? fullMetric.length() : endValueIndex);
  }

  /**
   * getOrCreate a metric.
   * TODO: Make this multi-threaded friendly?.
   * TODO: Do we need to create a new map for storing metric names separately?
   * TODO: Create a bitmap of max size 1 for full metric metricName. Does this help?
   * @param m the metric to assign a UUID
   * @return
   */
  @Override
  public int getOrCreate(final Metric m) {
    Optional<Integer> optionalMetric = get(m);
    return optionalMetric.isPresent() ? optionalMetric.get() : create(m);
  }

  @Override
  public String getMetricName(final int uuid) {
    return uuidMap.getOrDefault(uuid, MISSING_METRIC);
  }

  @Override
  public Map<String, Object> getStats() {
    Map<String, Object> stats = new HashMap<>();
    stats.put("NameMapSize", nameMap.size());
    stats.put("NameMapDistribution",
        nameMap.entrySet().stream().map(entry -> entry.getValue().getCardinality())
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting())));
    stats.put("UUIDMapSize", uuidMap.size());
    return stats;
  }

  /**
   * Set the maps to null explicitly so they can be GC'ed fast.
   */
  @Override
  public void close() {
    this.nameMap = null;
    this.uuidMap = null;
  }

  @VisibleForTesting
  Map<String, RoaringBitmap> getNameMap() {
    return nameMap;
  }

  public String getMetricNameFromId(final int id) {
    return uuidMap.get(id);
  }

  private int create(final Metric m) {
    int newMetricId = tagStoreCounter.incrementAndGet();

    // Add an entry with full metric metricName.
    addNameKey(m.fullMetricName, newMetricId);

    // Add an entry with metricName.
    addNameKey(m.metricName, newMetricId);

    // For a mapping for each key and value.
    for (Tag tag : m.tags) {
      addNameKey(tag.key, newMetricId);
      addNameKey(tag.rawTag, newMetricId);
    }

    // Add an inverse lookup for each metric Id so it's easy to get the metric metricName back.
    uuidMap.put(newMetricId, m.fullMetricName);
    return newMetricId;
  }

  private void addNameKey(final String key, final Integer id) {
    if (nameMap.containsKey(key)) {
      nameMap.get(key).add(id);
    } else {
      nameMap.put(key, RoaringBitmap.bitmapOf(id));
    }
  }
}
