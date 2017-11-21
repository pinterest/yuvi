package com.pinterest.yuvi.tagstore;

import static com.pinterest.yuvi.tagstore.Metric.TAG_DELIMITER;

import org.apache.commons.lang3.StringUtils;


/**
 * A tag is a key=value pair. The key and value must be ASCII.
 */
public final class Tag implements Comparable<Tag> {

  private static final String EMPTY_RAW_TAG = "=";

  public final String rawTag;
  public final String key;
  public final String value;

  public Tag(String key, String value, String rawTag) {
    if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)
        && StringUtils.isNotBlank(rawTag)) {
      this.key = key;
      this.value = value;
      this.rawTag = rawTag;
    } else {
      throw new IllegalArgumentException("Input values can't be blank :  "
          + key + " " + value + " " + rawTag);
    }
  }

  public Tag(String key, String value) {
    this(key, value, EMPTY_RAW_TAG);
  }

  @Override
  public int compareTo(Tag o) {
    int k = key.compareTo(o.key);
    if (k != 0) {
      return k;
    }
    return value.compareTo(o.value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Tag tag = (Tag) o;

    if (rawTag != null ? !rawTag.equals(tag.rawTag) : tag.rawTag != null) {
      return false;
    }
    if (key != null ? !key.equals(tag.key) : tag.key != null) {
      return false;
    }
    return value != null ? value.equals(tag.value) : tag.value == null;

  }

  @Override
  public int hashCode() {
    int result = rawTag != null ? rawTag.hashCode() : 0;
    result = 31 * result + (key != null ? key.hashCode() : 0);
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  /**
   * This function parses a key=value tag into this object. Each tag is a parsed using indexOf
   * instead of String.split for optimal performance. This code is called to parse every metric
   * ingested and on every query. So, it is very imporant to keep this code fast.
   *
   * @param rawTag
   * @return parsed Tag object
   */
  public static Tag parseTag(String rawTag) {
    int index = getDelimiterIndex(rawTag);
    String key = rawTag.substring(0, index);
    String value = rawTag.substring(index + 1);
    if (key.isEmpty() || value.isEmpty()) {
      throw new IllegalArgumentException("Invalid rawTag" + rawTag);
    }
    return new Tag(key, value, rawTag);
  }

  private static int getDelimiterIndex(String rawTag) {
    // Don't use string or split here since it has lower perf.
    int index = rawTag.indexOf(TAG_DELIMITER);
    if (index == -1) {
      throw new IllegalArgumentException("Invalid rawTag " + rawTag);
    }
    return index;
  }
}
