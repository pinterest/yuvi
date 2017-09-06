package com.pinterest.yuvi.chunk;

public enum QueryAggregation {
  NONE("none"),
  ZIMSUM("zimsum");

  private final String text;

  private QueryAggregation(final String text) {
    this.text = text;
  }

  @Override
  public String toString() {
    return text;
  }
}
