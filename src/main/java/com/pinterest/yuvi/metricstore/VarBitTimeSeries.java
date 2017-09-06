package com.pinterest.yuvi.metricstore;

import com.pinterest.yuvi.bitstream.BitStream;

import com.google.common.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * VarBitTimeSeries is able to compress a time series points into a binary format, using the
 * algorithm
 * described in this paper: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
 *
 * The points in the time series need not be sequential, and duplicate points are allowed. It should
 * be noted that the some storage efficiencies would be lost in the presense of non-sequential
 * inputs or in presence of duplicates.
 *
 * TODO: Merge timestamps and values into a single stream to improve data locality.
 *
 * Use the append interface to append the points and @CachingVarBitTimeSeriesIterator to read
 * them back.
 * @see CachingVarBitTimeSeriesIterator
 */
@VisibleForTesting
public class VarBitTimeSeries {

  public static final long MAX_UNIX_TIMESTAMP = Integer.MAX_VALUE;
  public static final int BLOCK_HEADER_OFFSET_SECS = 120 * 60; // 2 hour blocks.

  // Picking good initial sizes for these values will reduce GC overhead.
  public static final short DEFAULT_TIMESTAMP_BITSTREAM_SIZE = 1;
  public static final short DEFAULT_VALUE_BITSTREAM_SIZE = 1;

  private int size;

  private long prevTimestamp;
  private long prevTimestampDelta;
  private final BitStream timestamps;


  private int previousLeadingZeros;
  private int previousTrailingZeros;
  private long previousValue;
  private final BitStream values;

  public VarBitTimeSeries() {
    timestamps = new BitStream(DEFAULT_TIMESTAMP_BITSTREAM_SIZE);
    values = new BitStream(DEFAULT_VALUE_BITSTREAM_SIZE);
  }

  /**
   * Append a single point to the time series.
   *
   * @param timestamp a unix timestamp in seconds
   * @param value a floating-point value
   */
  public synchronized void append(long timestamp, double value) {
    if (timestamp < 0 || timestamp > MAX_UNIX_TIMESTAMP) {
      throw new IllegalArgumentException("Timestamp is not a valid unix timestamp: " + timestamp);
    }

    if (size == 0) {
      appendFirstPoint(timestamp, value);
    } else {
      appendNextPoint(timestamp, value);
    }
    size++;
  }

  private void appendNextPoint(long timestamp, double value) {
    appendTimestamp(timestamp);
    appendValue(value);
  }

  private void appendValue(double value) {
    long doubleToLongBits = Double.doubleToLongBits(value);

    long xorValue = doubleToLongBits ^ previousValue;

    int numberOfLeadingZeros = Long.numberOfLeadingZeros(xorValue);
    if (numberOfLeadingZeros >= 32) {
      numberOfLeadingZeros = 31;
    }

    int numberOfTrailingZeros = Long.numberOfTrailingZeros(xorValue);
    if (numberOfTrailingZeros >= 64) {
      numberOfTrailingZeros = 63;
    }

    if (xorValue == 0) {
      values.write(1, 0);
    } else if (numberOfLeadingZeros >= previousLeadingZeros
        && numberOfTrailingZeros >= previousTrailingZeros) {
      values.write(2, 0b10);
      values.write(64 - previousLeadingZeros - previousTrailingZeros,
          xorValue >>> previousTrailingZeros);
    } else {
      values.write(2, 0b11);
      values.write(5, numberOfLeadingZeros);
      int m = 64 - numberOfLeadingZeros - numberOfTrailingZeros;
      values.write(6, numberOfTrailingZeros);
      values.write(m, xorValue >>> numberOfTrailingZeros);
      previousTrailingZeros = numberOfTrailingZeros;
      previousLeadingZeros = numberOfLeadingZeros;
    }
    previousValue = doubleToLongBits;
  }

  private void appendTimestamp(long timestamp) {
    long delta = timestamp - prevTimestamp;
    long deltaOfDelta = delta - prevTimestampDelta;
    if (deltaOfDelta == 0) {
      timestamps.write(1, 0);
    } else if (deltaOfDelta >= -63 && deltaOfDelta <= 64) {
      timestamps.write(9, (deltaOfDelta + 63) | 0b100000000);
    } else if (deltaOfDelta >= -255 && deltaOfDelta <= 256) {
      timestamps.write(12, deltaOfDelta + 255 | 0b110000000000);
    } else if (deltaOfDelta >= -2047 && deltaOfDelta <= 2048) {
      timestamps.write(16, deltaOfDelta + 2047 | 0b1110000000000000);
    } else {
      timestamps.write(4, 0b1111);
      /**
       * There is a bug in gorilla algorithm where the delta of delta difference can't be encoded in
       * 32 bits. Adding such a value corrupts the time series encoding. However, this only happens
       * when a timestamp from 1970 is encoded followed by a value from 2016. Since the farthest
       * timestamps in a chunk will be apart from each other by a few hours, this case will never
       * happen. Instead of fixing the encoding and hurt the compression ratio, we throw an
       * exception in this case and reject the data point. Please refer to the test case for an
       * example.
       */
      if ((Math.log(deltaOfDelta + 2147483647) / Math.log(2)) > 32) {
        throw new IllegalArgumentException("timestamp overflows 32 bits: " + timestamp);
      }
      timestamps.write(32, deltaOfDelta + 2147483647);
    }
    prevTimestamp = timestamp;
    prevTimestampDelta = delta;
  }

  /**
   * Starting timestamp is at the 2 hour starting window of the current timestamp. The first
   * timestamp is encoded as a delta from the starting timestamp in 14 bits.
   */
  private void appendFirstPoint(long timestamp, double value) {
    long twoHourTimestampOverage = timestamp % BLOCK_HEADER_OFFSET_SECS;
    long blockHeaderTimestamp = timestamp - twoHourTimestampOverage;
    timestamps.write(32, blockHeaderTimestamp);
    prevTimestamp = timestamp;
    prevTimestampDelta = prevTimestamp - blockHeaderTimestamp;
    timestamps.write(14, prevTimestampDelta);

    // Store first value with no compression.
    long longValue = Double.doubleToLongBits(value);
    values.write(64, longValue);
    previousValue = longValue;
    previousLeadingZeros = 64;
    previousTrailingZeros = 64;
  }

  /**
   * Read a snapshot of the time series data that has been written.
   * @return an object that can deserialize the compressed data.
   */
  public synchronized TimeSeriesIterator read() {
    //TODO: The read object returns the values at a point instead of returning all values when
    // called. Change it.
    return new CachingVarBitTimeSeriesIterator(size, timestamps.read(), values.read());
  }

  @VisibleForTesting
  int getSize() {
    return size;
  }

  public Map<String, Double> getStats() {
    Map<String, Double> stats = new HashMap<>();
    stats.put("pointsCount", new Double(size));
    timestamps.getStats().entrySet().forEach(
        entry -> stats.put("timestamps_" + entry.getKey(), entry.getValue()));
    values.getStats().entrySet().forEach(
        entry -> stats.put("values_" + entry.getKey(), entry.getValue()));
    return Collections.unmodifiableMap(stats);
  }

  @VisibleForTesting
  BitStream getTimestamps() {
    return timestamps;
  }

  @VisibleForTesting
  BitStream getValues() {
    return values;
  }

  public int getSerializedByteSize() {
    return Integer.BYTES  // Size of counter
        + timestamps.getSerializedByteSize()  // Size of timestamps bit stream
        + values.getSerializedByteSize();  // Size of values
  }

  public void serialize(ByteBuffer buffer) throws Exception {
    buffer.putInt(size);
    timestamps.serialize(buffer);
    values.serialize(buffer);
  }

  public static TimeSeriesIterator deserialize(final ByteBuffer buffer) {
    try {
      int size = buffer.getInt();
      BitStream timestamps = BitStream.deserialize(buffer);
      BitStream values = BitStream.deserialize(buffer);
      return new CachingVarBitTimeSeriesIterator(size, timestamps.read(), values.read());
    } catch (Exception e) {
      return null;
    }
  }
}
