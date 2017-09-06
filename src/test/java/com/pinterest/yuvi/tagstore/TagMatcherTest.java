package com.pinterest.yuvi.tagstore;

import static org.junit.Assert.assertEquals;

import junit.framework.TestCase;
import org.junit.Test;

public class TagMatcherTest {

  private final String testKey = "testKey";
  private final String testValue = "testValue";
  private final Tag testTag = new Tag(testKey, testValue, testKey + "=" + testValue);

  @Test
  public void testTagMatcherCreation() {

    TagMatcher m2 = new TagMatcher(MatchType.WILDCARD, testTag);
    assertEquals(testTag, m2.tag);
    assertEquals(MatchType.WILDCARD, m2.type);

    TagMatcher m4 = TagMatcher.wildcardMatch(testKey, "*");
    assertEquals(new Tag(testKey, "*"), m4.tag);
    assertEquals(MatchType.WILDCARD, m4.type);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullType() {
    new TagMatcher(null, testTag);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullTag() {
    new TagMatcher(MatchType.NOT_ILITERAL_OR, null);
  }

  @Test
  public void testExactTagMatcher() {
    TagMatcher m1 = new TagMatcher(MatchType.EXACT, testTag);
    assertEquals(testTag, m1.tag);
    assertEquals(MatchType.EXACT, m1.type);
  }
}
