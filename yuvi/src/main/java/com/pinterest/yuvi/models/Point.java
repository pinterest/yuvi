package com.pinterest.yuvi.models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An immutable floating-point value at a single point in time. Second-level resolution.
 */
public class Point {

  private final long ts;
  private final double val;

  /**
   * Create a new point object.
   * @param ts unix timestamp in seconds.
   * @param val arbitrary value, corresponding to a measurement at the given timestamp.
   */
  public Point(long ts, double val) {
    this.ts = ts;
    this.val = val;
  }

  public long getTs() {
    return ts;
  }

  public double getVal() {
    return val;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Point point = (Point) o;

    if (ts != point.ts) {
      return false;
    }
    if (Double.compare(point.val, val) != 0) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = (int) (ts ^ (ts >>> 32));
    temp = Double.doubleToLongBits(val);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "Point{"
        + "ts=" + ts
        + ", val=" + val
        + '}';
  }
}
