package com.pinterest.yuvi.models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility classes to work on a set of points.
 */
public class Points {

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

    ArrayList<Point> sortedPoints = new ArrayList<Point>(points);
    // Stable sort
    Collections.sort(sortedPoints, (Point a, Point b) -> (int) (a.getTs() - b.getTs()));
    int fp;
    int bp;
    for (fp = 1, bp = 0; fp < sortedPoints.size(); fp++) {
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
