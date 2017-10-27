package com.pinterest.yuvi.metricstore;

import com.pinterest.yuvi.models.Point;

import java.util.List;

/**
 * An iterator interface for reading the time series data.
 */
public interface TimeSeriesIterator {

  List<Point> getPoints();
}
