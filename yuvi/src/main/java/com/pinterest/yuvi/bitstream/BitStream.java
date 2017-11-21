package com.pinterest.yuvi.bitstream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A class that stores a sequence of bits in a binary array. Bits are written sequentially. The
 * underlying array grows automatically. This class is not thread safe.
 * TODO: Bounds checks on capacity, index and shift
 * TODO: Tweak initial capacity to reduce resize ops.
 */
public class BitStream {

  static final int DEFAULT_INITIAL_CAPACITY = 16;

  private static final double DEFAULT_CAPACITY_MULTIPLE = 2;

  private long[] data;
  private int capacity;
  private int index;
  private byte shift;

  BitStream() {
    this(DEFAULT_INITIAL_CAPACITY);
  }

  public BitStream(int initialCapacity) {
    this(new long[initialCapacity], initialCapacity,  0, (byte) 0);
  }

  public BitStream(long[] data, int len, int index, byte shift) {
    this.data = data;
    this.capacity = len;
    this.index = index;
    this.shift = shift;
  }

  /**
   * Return the number of bits that have been written.
   * @return number of bits written.
   */
  int bitOffset() {
    return index * 64 + shift;
  }

  private long bitCapacity() {
    return capacity * 64;
  }

  private void resize() {
    int newSize =
        new Double(Math.floor((double) capacity * DEFAULT_CAPACITY_MULTIPLE)).intValue();

    long[] newdata = new long[newSize];
    for (int i = 0; i < capacity; i++) {
      newdata[i] = data[i];
    }

    capacity = newSize;
    data = newdata;
  }

  private void reserve(int n) {
    while (bitCapacity() - bitOffset() < n) {
      resize();
    }
  }

  /**
   * Append up to 64 bits to the array.
   * @param n the number of bits to append. Between 0 and 64.
   * @param v an integer containing the bits. The n least-significant bits are used.
   * TODO: test for value overflow also.
   */
  public void write(int n, long v) {
    if (n < 1 || n > 64) {
      throw new IllegalArgumentException(
          String.format("Unable to write %s bits to value %d", n, v));
    }

    reserve(n);
    long v1 = v << 64 - n >>> shift;
    data[index] = data[index] | v1;
    shift += n;
    if (shift >= 64) {
      shift -= 64;
      index++;
      if (shift != 0) {
        long v2 = v << 64 - shift;
        data[index] = data[index] | v2;
      }
    }
  }

  public Map<String, Double> getStats() {
    HashMap<String, Double> stats = new HashMap<>();
    stats.put("dataLength", new Double(index));
    stats.put("dataSize", new Double(getSerializedByteSize()));
    stats.put("capacity", new Double(capacity));
    return Collections.unmodifiableMap(stats);
  }

  /**
   * Return an object to read the bit stream. Bits are immutable after they are written, so the
   * returned reader may be used on a separate thread.
   * @return a reader pointing to the data that has been written.
   */
  public BitStreamIterator read() {
    return new BitStreamIterator(data, bitOffset());
  }

  /**
   * Construct a new BitReader using the data in the given ByteBuffer.
   * @param buffer a buffer containing the data
   * @throws Exception if the parsing failed.
   * @return a new BitReader
   */
  public static BitStream deserialize(ByteBuffer buffer) throws Exception {
    int validDataSize = buffer.getInt();
    byte shift = buffer.get();
    long[] data = new long[validDataSize];
    for (int i = 0; i < validDataSize; i++) {
      data[i] = buffer.getLong();
    }
    int index = shift == 0 ? validDataSize : validDataSize - 1;
    return new BitStream(data, validDataSize, index, shift);
  }

  /**
   * Write the data to a pre-allocated ByteBuffer.
   * @param buffer must have capacity greater or equal to serializedSize
   * @throws Exception if buffer is invalid
   */
  public void serialize(ByteBuffer buffer) throws Exception {
    int validDataSize = getLastDataIndex();
    buffer.putInt(validDataSize);
    buffer.put(shift);
    for (int i = 0; i < validDataSize; i++) {
      buffer.putLong(data[i]);
    }
  }

  public int getSerializedByteSize() {
    return Ints.BYTES  // Size of index.
        + Byte.BYTES +  // Size of shift
        Long.BYTES * getLastDataIndex();  // Size of long valid data
  }

  private int getLastDataIndex() {
    return shift == 0 ? index : index + 1;
  }

  @VisibleForTesting
  long[] getData() {
    return data;
  }

  @VisibleForTesting
  int getCapacity() {
    return capacity;
  }

  @VisibleForTesting
  int getIndex() {
    return index;
  }

  @VisibleForTesting
  byte getShift() {
    return shift;
  }
}
