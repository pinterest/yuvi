package com.pinterest.yuvi.models;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.List;

/**
 * Utility classes to work on a set of points.
 */
public class Points {
  private Points() {
    // private constructor for utility class.
  }

  /**
   * Sort the points by timestamp and remove data points with duplicate
   * timestamps. If two data points have the same timestamp, the second one gets
   * precedence.
   * @param points a list of points
   */
  public static List<Point> dedup(List<Point> points) {
    if (points.isEmpty()) {
      return points;
    }

    List<Point> sortedPoints = new ArrayList<>(points);
    // Stable sort
    Collections.sort(sortedPoints, Comparator.comparingLong(Point::getTs));
    int bp = 0;
    for (int fp = 1; fp < sortedPoints.size(); fp++) {
      if (sortedPoints.get(fp).getTs() == sortedPoints.get(bp).getTs()) {
        sortedPoints.set(bp, sortedPoints.get(fp));
      } else {
        bp++;
        sortedPoints.set(bp, sortedPoints.get(fp));
      }
    }
    return sortedPoints.subList(0, bp + 1);
  }
}
