package com.pinterest.yuvi.models;

import java.util.List;

public class TimeSeries {

  private final String metric;
  private final List<Point> points;

  public TimeSeries(String metric, List<Point> points) {
    if (metric == null || metric.isEmpty()) {
      throw new IllegalArgumentException("Metric name can't be null or empty");
    }
    if (points == null) {
      throw new IllegalArgumentException("points can't be null");
    }
    this.metric = metric;
    this.points = points;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimeSeries that = (TimeSeries) o;

    if (metric != null ? !metric.equals(that.metric) : that.metric != null) {
      return false;
    }
    return points != null ? points.equals(that.points) : that.points == null;
  }

  @Override
  public int hashCode() {
    int result = metric != null ? metric.hashCode() : 0;
    result = 31 * result + (points != null ? points.hashCode() : 0);
    return result;
  }

  public List<Point> getPoints() {
    return points;
  }

  public String getMetric() {
    return metric;

  }

  @Override
  public String toString() {
    return "TimeSeries{" + "metric='" + metric + '\'' + ", points=" + points + '}';
  }
}
