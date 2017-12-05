package com.pinterest.yuvi.tagstore;

import com.google.common.annotations.VisibleForTesting;
import net.openhft.chronicle.map.ChronicleMap;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
 * A Tag store needs to perform the following tasks:
 * 1) On ingest assign a unique id to each metric (metric_name + sorted tags).
 * 2) On query, return a list of ids that match the given query.
 * 3) Given a metric id, return the name of the metric.
 *
 * To achieve this, this class implements the tag store interface using an inverted index and a
 * forward index. The forward index will assign a metric id to each unique metric, useful for (1)
 * and (3) above. Since the forward index contains a large quantitiy of strings, for GC efficiency,
 * we store this map in off heap memory.
 *
 * The inverted index will help with (2). It stores a mapping of strings to a list of metric ids.
 * For efficiency, the list of metric ids are stored in a roaring bitmap. Each query is translated
 * into a series of lookups and operations on a roaring bitmaps. For now, the metric index is on
 * heap because there is a cost to serialize and de-serialize the roaring bitmap every time a metric
 * is added or removed and it is not expected to be small. We also don't expect the index to be
 * persisted to disk because it can be re-constructed from the metricIdMap.
 */
public class InvertedIndexTagStore implements TagStore {

  private static Logger LOG = LoggerFactory.getLogger(InvertedIndexTagStore.class);

  private static final int DEFAULT_METRIC_ID_MAP_SIZE = 10000;
  private static final int DEFAULT_METRIC_INDEX_SIZE = DEFAULT_METRIC_ID_MAP_SIZE;
  private static final int AVERAGE_METRIC_NAME_SIZE = 250;
  private static final boolean USE_OFFHEAP_TAGSTORE = true;

  private static final String MISSING_METRIC = "";

  private static final RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();

  private Map<String, RoaringBitmap> metricIndex;
  private Map<Integer, String> metricIdMap;
  private AtomicInteger tagStoreCounter;
  private final int metricIdMapCapacity;

  public InvertedIndexTagStore() {
    this(DEFAULT_METRIC_ID_MAP_SIZE, DEFAULT_METRIC_INDEX_SIZE);
  }

  public InvertedIndexTagStore(int metricIdMapCapacity, int initialIndexSize) {
    this(metricIdMapCapacity, initialIndexSize, "");
  }

  public InvertedIndexTagStore(int metricIdMapCapacity, int initialIndexSize,
                               String dataDirectory) {

    LOG.info("Creating an inverted index tag store.");
    if (USE_OFFHEAP_TAGSTORE) {
      if (!dataDirectory.isEmpty()) {
        File offHeapFile = new File(dataDirectory + "/tagStore_metricIdMap");
        try {
          this.metricIdMap = ChronicleMap.of(Integer.class, String.class)
              .entries(metricIdMapCapacity)
              .averageValueSize(AVERAGE_METRIC_NAME_SIZE)
              .name("tagStore")
              .createPersistedTo(offHeapFile);
          LOG.info("Created an off heap tag store of size={} valueSize={} and persisted at {}",
              metricIdMapCapacity, AVERAGE_METRIC_NAME_SIZE, offHeapFile.toString());
        } catch (IOException e) {
          LOG.error("Failed to create an offheap store {} with error {}", offHeapFile,
              e.getMessage());
          throw new IllegalArgumentException("Failed to create an off heap store.", e);
        }
      } else {
        this.metricIdMap = ChronicleMap.of(Integer.class, String.class)
            .entries(metricIdMapCapacity)
            .averageValueSize(AVERAGE_METRIC_NAME_SIZE)
            .create();
        LOG.info("Created an off heap tag store of size={} valueSize={}",
            metricIdMapCapacity, AVERAGE_METRIC_NAME_SIZE);
      }
    } else {
      this.metricIdMap = new ConcurrentHashMap<>(metricIdMapCapacity);
      LOG.info("Created an on heap tag store with capacity {}", metricIdMapCapacity);
    }

    this.metricIndex = new ConcurrentHashMap<>(initialIndexSize);

    this.tagStoreCounter = new AtomicInteger(1);
    this.metricIdMapCapacity = metricIdMapCapacity;
    LOG.info("Created an inverted index tag store.");
  }

  @Override
  public Optional<Integer> get(Metric m) {
    if (metricIndex.containsKey(m.fullMetricName)) {
      return Optional.of(metricIndex.get(m.fullMetricName).getIntIterator().next());
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

    if (metricIndex.containsKey(metricName)) {
      andBitMaps.add(metricIndex.get(metricName));
    } else {
      andBitMaps.add(EMPTY_BITMAP); // If no metric name is present, return empty bitmap
    }

    for (TagMatcher t : tagMatchers) {
      if (metricIndex.containsKey(t.tag.key)) {
        // Include all metrics that include this tag key.
        andBitMaps.add(metricIndex.get(t.tag.key));

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

    if (metricIndex.containsKey(metricName)) {
      andNotBitMaps.add(metricIndex.get(metricName));
    } else {
      andNotBitMaps.add(EMPTY_BITMAP);
    }

    for (TagMatcher t : tagMatchers) {
      if (metricIndex.containsKey(t.tag.key)) {
        // Include all metrics that include this tag key.
        andNotBitMaps.add(metricIndex.get(t.tag.key));

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
    if (metricIndex.containsKey(rawTag)) {
      resultMap.add(metricIndex.get(rawTag));
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
        if (metricIndex.containsKey(tagValuePair)) {
          orMaps.add(metricIndex.get(tagValuePair));
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

    if (metricIndex.containsKey(metricName)) {
      andBitMaps.add(metricIndex.get(metricName));
    } else {
      andBitMaps.add(EMPTY_BITMAP);
    }

    if (metricIndex.containsKey(key)) {
      andBitMaps.add(metricIndex.get(key));
    } else {
      andBitMaps.add(EMPTY_BITMAP);
    }

    RoaringBitmap resultBitMap = FastAggregation.and(andBitMaps.iterator());
    HashMap<Integer, String> resultMap = new HashMap<>();
    for (int i : resultBitMap.toArray()) {
      resultMap.put(i, extractTagValueForTagKey(metricIdMap.get(i), key));
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
   * get the id of a metric if it exists or create an id for the metric and return it.
   * TODO: Make this multi-threaded friendly.
   * @param m the metric to assign a metricId
   * @return
   */
  @Override
  public int getOrCreate(final Metric m) {
    Optional<Integer> optionalMetric = get(m);
    return optionalMetric.isPresent() ? optionalMetric.get() : create(m);
  }

  @Override
  public String getMetricName(final int metricId) {
    return metricIdMap.getOrDefault(metricId, MISSING_METRIC);
  }

  @Override
  public Map<String, Object> getStats() {
    Map<String, Object> stats = new HashMap<>();
    stats.put("IndexMapSize", metricIndex.size());
    stats.put("IndexMapDistribution",
        metricIndex.entrySet().stream().map(entry -> entry.getValue().getCardinality())
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting())));
    stats.put("MetricIdMapSize", metricIdMap.size());
    return stats;
  }

  /**
   * Set the maps to null explicitly so they can be GC'ed fast.
   */
  @Override
  public void close() {
    metricIdMap.clear();
    this.metricIdMap = null;
    metricIndex.clear();
    this.metricIndex = null;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @VisibleForTesting
  Map<String, RoaringBitmap> getMetricIndex() {
    return metricIndex;
  }

  public String getMetricNameFromId(final int id) {
    return metricIdMap.get(id);
  }

  /**
   * Add a new metric to tag store. This method will assign an id to the metric and add it to the
   * metric index.
   *
   * Failure to add a metric to metricIndex will leave the tag store in an inconsistent state.
   *
   * TODO: Add revert action to this method if metric creation fails.
   * TODO: Consider preventing further writes to the tag store once we hit capacity.
   */
  private int create(final Metric m) {
    int newMetricId = tagStoreCounter.incrementAndGet();

    /**
     * Add an inverse lookup for each metric Id so it's easy to get the metric metricName back.
     *
     * We should update the metricIdMap first because if it fails the metricIndex will be in an
     * inconsistent state since we are adding a metricId that was never created. Another reason is
     * since metricIdMap is off heap there is a higher chance of failures than
     * adding an element to the in-memory map.
     */
    metricIdMap.put(newMetricId, m.fullMetricName);

    // Add an entry with full metric metricName.
    addToMetricIndex(m.fullMetricName, newMetricId);

    // Add an entry with metricName.
    addToMetricIndex(m.metricName, newMetricId);

    // For a mapping for each key and value.
    for (Tag tag : m.tags) {
      addToMetricIndex(tag.key, newMetricId);
      addToMetricIndex(tag.rawTag, newMetricId);
    }

    // If the off heap tag store exceeds capacity print a warning once to prevent log spam. In
    // practice, we can take up to 2x the number of keys than capacity, so this is not fatal yet.
    // But once we hit 99% of capacity, chronicle map recommends a resize, so we should act when
    // we see this warning. The off-heap is never re-sized. It can take more elements than its
    // capacity because it pre-allocates a few slabs of memory for hash map and extends a slab when
    // it's full until the map has reached capacity. This leads to slower hash lookup and insert
    // performance. Once the capacity limit is hit it doesn't allocate any more slabs but inserts
    // elements until the currently allocated slabs are full. So, you can insert more elements than
    // the capacity but it can run over anytime once we hit the limit. Hence the a warning to resize
    // the off heap. More info at: https://github.com/OpenHFT/Chronicle-Map/tree/master/spec
    if (USE_OFFHEAP_TAGSTORE && metricIdMap.size() == metricIdMapCapacity) {
      LOG.warn("The off heap tag store has reached it's capacity of {}. Resize it.",
          metricIdMapCapacity);
    }

    return newMetricId;
  }

  private void addToMetricIndex(final String key, final Integer id) {
    if (metricIndex.containsKey(key)) {
      metricIndex.get(key).add(id);
    } else {
      metricIndex.put(key, RoaringBitmap.bitmapOf(id));
    }
  }
}
