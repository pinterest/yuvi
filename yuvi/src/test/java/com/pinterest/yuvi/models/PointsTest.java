package com.pinterest.yuvi.models;

import junit.framework.TestCase;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PointsTest extends TestCase {

  public void testDedup() {
    Point point1 = new Point(1, 1);
    Point point2 = new Point(2, 2);
    Point point3 = new Point(3, 3);

    List<Point> points = Collections.unmodifiableList(Arrays.asList(point1, point2, point3));
    List<Point>
        reversedPoints =
        Collections.unmodifiableList(Arrays.asList(point3, point2, point1));

    assertTrue(Points.dedup(Collections.emptyList()).isEmpty());
    Assert.assertThat(points, IsIterableContainingInOrder.contains(Points.dedup(points).toArray()));
    Assert.assertThat(points,
        IsIterableContainingInOrder.contains(Points.dedup(reversedPoints).toArray()));

    List<Point> duplicatePoints = Arrays.asList(point1, new Point(1, 2));
    List<Point> result1 = Points.dedup(duplicatePoints);
    assertEquals(1, result1.size());
    assertEquals(1, result1.get(0).getTs());
    assertEquals(2.0, result1.get(0).getVal());
    assertEquals(new Point(1, 2), result1.get(0));

    List<Point> duplicatePoints2 = Arrays.asList(point1, new Point(2, 3), point2, point3);
    Assert.assertThat(points,
        IsIterableContainingInOrder.contains(Points.dedup(duplicatePoints2).toArray()));

    List<Point> duplicatePoints3 = Arrays.asList(point1, point2, new Point(2, 3), point2, point3);
    Assert.assertThat(points,
        IsIterableContainingInOrder.contains(Points.dedup(duplicatePoints3).toArray()));

    List<Point> duplicatePoints4 = Arrays.asList(point1, new Point(1, 2), new Point(1, -1));
    List<Point> result2 = Points.dedup(duplicatePoints4);
    assertEquals(1, result2.size());
    assertEquals(new Point(1, -1), result2.get(0));
  }
}
