package com.pinterest.yuvi.tagstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TagTest {

  @Test
  public void testTagCreation() {
    testInvalidTag(null, "1", "1");
    testInvalidTag("1", null, "1");
    testInvalidTag("1", "1", null);
    testInvalidTag("", "1", "1");
    testInvalidTag("1", "", "1");
    testInvalidTag("1", "1", "");

    Tag t = new Tag("k", "v", "tag");
    assertEquals("k", t.key);
    assertEquals("v", t.value);
    assertEquals("tag", t.rawTag);
  }

  private void testInvalidTag(String key, String value, String tag) {
    try {
      new Tag(key, value, tag);
      fail("Should fail with exception");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testTagParse() {
    testInvalidTagParse("a");
    testInvalidTagParse("a=");
    testInvalidTagParse("=a");
    testInvalidTagParse("=");

    Tag t = Tag.parseTag("k=v");
    assertEquals("k", t.key);
    assertEquals("v", t.value);
    assertEquals("k=v", t.rawTag);

    Tag t1 = Tag.parseTag("k=v=1");
    assertEquals("k", t1.key);
    assertEquals("v=1", t1.value);
    assertEquals("k=v=1", t1.rawTag);
  }

  private void testInvalidTagParse(String tag) {
    try {
      Tag.parseTag(tag);
      fail("Should fail with exception");
    } catch (IllegalArgumentException e) {
    }
  }
}
