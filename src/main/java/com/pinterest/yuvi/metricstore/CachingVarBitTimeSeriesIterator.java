package com.pinterest.yuvi.metricstore;

import com.pinterest.yuvi.models.Point;
import com.pinterest.yuvi.bitstream.BitStreamIterator;
import com.pinterest.yuvi.bitstream.ParseException;

import java.util.ArrayList;
import java.util.List;

/**
 * CachingVarBitTimeSeriesIterator is able to deserialize a delta-compressed time series into a
 * list of
 * points. The iterator can only read the bits at the time it is created.
 *
 * TODO: Pass in a reference to delta series to this iterator.
 * TODO: Implement Java iterator interface.
 * TODO: Separate the caching iterator and a non-caching iterator.
 * @see VarBitTimeSeries
 */
public class CachingVarBitTimeSeriesIterator implements TimeSeriesIterator {

  private final int count;
  private final BitStreamIterator timestamps;
  private final BitStreamIterator values;
  private long prevTimestamp;
  private long previousDelta;
  private int prevNumberOfLeadingZeros;
  private int prevNumberOfTrailingZeros;
  private long prevValue;
  private int idx;

  /**
   * Create an iterator to read a given delta time series store.
   * @param count the number of items in the time series.
   * @param timestamps the compressed timestamps.
   * @param values the compressed values.
   */
  public CachingVarBitTimeSeriesIterator(int count, BitStreamIterator timestamps,
                                         BitStreamIterator values) {
    this.count = count;
    this.timestamps = timestamps;
    this.values = values;
  }

  private void readFirst(List<Point> pointList) {
    long t2 = timestamps.read(32);
    previousDelta = timestamps.read(14);
    prevTimestamp = t2 + previousDelta;

    prevValue = values.read(64);
    double v = Double.longBitsToDouble(prevValue);

    pointList.add(new Point(prevTimestamp, v));

    idx++;
  }

  private void readNext(List<Point> pointList) {
    long deltaOfDelta;
    if (timestamps.tryRead(1, 0)) {
      deltaOfDelta = 0;
    } else if (timestamps.tryRead(2, 0b10)) {
      deltaOfDelta = timestamps.read(7) - 63;
    } else if (timestamps.tryRead(3, 0b110)) {
      deltaOfDelta = timestamps.read(9) - 255;
    } else if (timestamps.tryRead(4, 0b1110)) {
      deltaOfDelta = timestamps.read(12) - 2047;
    } else {
      timestamps.read(4);
      deltaOfDelta = timestamps.read(32) - 2147483647;
    }
    long delta = deltaOfDelta + previousDelta;
    long timeStamp = delta + prevTimestamp;
    prevTimestamp = timeStamp;
    previousDelta = delta;

    long xorValue;
    if (values.tryRead(1, 0)) {
      xorValue = 0;
    } else {
      long prefix = values.read(2);
      if (prefix == 0b11) {
        prevNumberOfLeadingZeros = (int) values.read(5);
        prevNumberOfTrailingZeros = (int) values.read(6);
      }
      int n = 64 - prevNumberOfLeadingZeros - prevNumberOfTrailingZeros;
      if (n < 0 || n > 64) {
        throw new ParseException("Invalid number of meaningful bits: " + n);
      }
      xorValue = values.read(n) << prevNumberOfTrailingZeros;
    }
    long v = prevValue ^ xorValue; // TODO: Encode -Inf into long turns it into NaN?
    double val = Double.longBitsToDouble(v);
    prevValue = v;

    pointList.add(new Point(timeStamp, val));

    idx++;
  }

  /**
   * Perform the decompression. May only be called once.
   * @return the decompressed time series.
   */
  public List<Point> getPoints() {
    List<Point> pointList = new ArrayList<>(count);
    if (count == 0) {
      return pointList;
    }
    readFirst(pointList);
    while (idx < count) {
      readNext(pointList);
    }
    return pointList;
  }
}
