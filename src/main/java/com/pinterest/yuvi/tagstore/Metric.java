package com.pinterest.yuvi.tagstore;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A ASCII metric metricName. The metric name contains a metric name and a list of tags. Each of the
 * tag is of the form key=value.
 */
public final class Metric {

  public static final char TAG_DELIMITER = '=';
  public static final char METRIC_SEPARATOR = ' ';

  public final String metricName;

  public final List<Tag> tags;

  public final String fullMetricName;

  /**
   * A metric class to store the metric.
   * raw tags are key=value ascii string pairs.
   *
   * Pre-compute fullMetricName since it is needed by invertedTagStore and is also useful for
   * debugging. In future, this field
   *
   * If there is memory pressure, consider storing rawTags instead of Tag objects.
   *
   * @param metricName
   * @param rawTags
   */
  public Metric(final String metricName, final List<String> rawTags) {
    if (StringUtils.isNotBlank(metricName)) {
      this.metricName = metricName;
    } else {
      throw new IllegalArgumentException("Invalid metric name");
    }

    try {
      this.tags = parseTags(rawTags);
    } catch (Exception e) {
      throw new IllegalArgumentException("Error parsing tags" + rawTags, e);
    }

    this.fullMetricName = getFullMetricName(rawTags);
  }

  /**
   * Parse the tags and return a list of tags. The purpose of this method is to provide a single
   * method for parsing the tags.
   * Tag class can validate the tag key and value format.
   * @param rawTags a list of strings of the form "tag1=value1 tag2=value2..."
   */
  public static List<Tag> parseTags(List<String> rawTags) {
    return rawTags.stream().filter(rawTag -> rawTag != null && !rawTag.isEmpty())
        .map(rawTag -> Tag.parseTag(rawTag.trim())).collect(Collectors.toList());
  }

  private String getFullMetricName(List<String> rawTags) {
    List<String> filteredRawTags = rawTags.stream()
        .filter(tag -> tag != null && !tag.isEmpty())
        .map(tag -> tag.trim())
        .collect(Collectors.toList());

    Collections.sort(filteredRawTags);
    if (rawTags.isEmpty()) {
      return metricName;
    } else {
      return new StringBuilder().append(metricName)
          .append(METRIC_SEPARATOR)
          .append(String.join(String.valueOf(METRIC_SEPARATOR), filteredRawTags))
          .toString();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Metric metric = (Metric) o;

    if (metricName != null ? !metricName.equals(metric.metricName) : metric.metricName != null) {
      return false;
    }
    if (tags != null ? !tags.equals(metric.tags) : metric.tags != null) {
      return false;
    }
    return fullMetricName != null ? fullMetricName.equals(metric.fullMetricName)
                                  : metric.fullMetricName == null;

  }

  @Override
  public int hashCode() {
    int result = metricName != null ? metricName.hashCode() : 0;
    result = 31 * result + (tags != null ? tags.hashCode() : 0);
    result = 31 * result + (fullMetricName != null ? fullMetricName.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Metric{"
        + "metricName='" + metricName + '\''
        + ", tags=" + tags
        + ", fullMetricName='" + fullMetricName + '\''
        + '}';
  }
}
