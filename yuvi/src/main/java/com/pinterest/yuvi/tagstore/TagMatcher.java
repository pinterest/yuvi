package com.pinterest.yuvi.tagstore;

import org.apache.commons.lang3.StringUtils;

/*
 * TagMatcher contains data about matching a tag with other tags in a query. It only contains config
 * data for a tag matcher but the actual implementation of tag matching happens in the tag store.
 * implementation of tag matching occurs inside the TagStore.
 */
public final class TagMatcher {

  public static final String WILDCARD = "*";
  public final MatchType type;
  public final Tag tag;

  public TagMatcher(MatchType type, Tag tag) {
    if (type == null || tag == null) {
      throw new IllegalArgumentException("Tag or type can't be null");
    }
    this.type = type;
    this.tag = tag;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TagMatcher that = (TagMatcher) o;

    if (type != that.type) {
      return false;
    }
    return tag != null ? tag.equals(that.tag) : that.tag == null;

  }

  @Override
  public int hashCode() {
    int result = type != null ? type.hashCode() : 0;
    result = 31 * result + (tag != null ? tag.hashCode() : 0);
    return result;
  }

  /**
   * Exact matches all tag sets that contain a tag that exactly matches the given key and
   * value.
   * @param tag tag to match
   * @return a new TagMatcher object
   */
  public static TagMatcher exactMatch(Tag tag) {
    return new TagMatcher(MatchType.EXACT, tag);
  }

  /**
   * Wildcard matches all tag sets that contain a tag that exactly matches the given key.
   * @param key the key to lookup
   * @param wildcardString
   * @return a new TagMatcher object
   */
  public static TagMatcher wildcardMatch(String key, String wildcardString) {
    return createWildCardTagMatcher(key, wildcardString, MatchType.WILDCARD);
  }

  private static TagMatcher createWildCardTagMatcher(String key, String wildcardString,
                                                     MatchType matchType) {

    if (StringUtils.isBlank(key) || StringUtils.isBlank(wildcardString)) {
      throw new IllegalArgumentException("Key or wildcard string cannot be null or empty");
    }

    if (!wildcardString.contains(WILDCARD)) {
      throw new IllegalArgumentException("Wildcard string must contain an asterisk");
    }

    return new TagMatcher(matchType, new Tag(key, wildcardString));
  }

  public static TagMatcher iwildcardMatch(String key, String wildcardString) {
    return createWildCardTagMatcher(key, wildcardString, MatchType.IWILDCARD);
  }

  public static TagMatcher literalOrMatch(String key, String orLiteralString,
                                          boolean caseInsensitive) {

    return new TagMatcher(caseInsensitive ? MatchType.ILITERAL_OR : MatchType.LITERAL_OR,
        new Tag(key, orLiteralString));
  }

  public static TagMatcher notLiteralOrMatch(String key, String orLiteralString,
                                             boolean caseInsensitive) {

    return new TagMatcher(caseInsensitive ? MatchType.NOT_ILITERAL_OR : MatchType.NOT_LITERAL_OR,
        new Tag(key, orLiteralString));
  }

  public static TagMatcher regExMatch(String key, String regExString) {
    return new TagMatcher(MatchType.REGEXP, new Tag(key, regExString));
  }

  @Override
  public String toString() {
    return "TagMatcher{" + "type=" + type + ", tag=" + tag + '}';
  }
}
