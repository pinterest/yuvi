package com.pinterest.yuvi.bitstream;

import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;

/**
 * Provides functions for reading a sequence of raw bits from a binary array. Bits are read
 * sequentially, and are addressed by their offset from the beginning of the array. Not thread
 * safe.
 *
 * TODO: Take the BitStream object as input for this class.
 * TODO: Implement Java Iterator interface.
 * TODO: Investigate if is long array is the best or if a int array if more performant.
 */
public class BitStreamIterator {

  private final long[] data;
  private final int bitLength;
  private int idx;
  private byte shift;

  /**
   * Construct a reader that starts reading from the beginning of the array.
   * @param data the raw binary data, big-endian
   * @param bitLength the number of bits in the array. Attempts to access bits at offset >
   *                  bitLength will
   *            result in a ParseException.
   */
  BitStreamIterator(long[] data, int bitLength) {
    this.data = data;
    this.bitLength = bitLength;
  }

  /**
   * Get the offset of the next bit to read.
   * @return the offset from the beginning of the array.
   */
  int bitOffset() {
    return idx * 64 + shift;
  }

  /**
   * Read up to 64 consecutive bits from the array.
   * @param n number of bits to read, between 0 and 64.
   * @return a long integer containing the bits in the n least-significant positions.
   * @throws ParseException if the reader has reached the end of the bit array.
   */
  public long read(int n) {
    if (n < 0 && n > 64) {
      throw new IllegalArgumentException(n + " should be less than 64 bits.");
    }

    int want = bitOffset() + n;
    if (want > bitLength) {
      throw new ParseException("Out of bounds: bitLength=" + bitLength + " want=" + want);
    }

    long result;
    if (64 - shift > n) {
      result = data[idx] << shift >>> 64 - n;
      shift += n;
    } else {
      result = data[idx] << shift >>> shift;
      shift += n;
      if (shift >= 64) {
        shift -= 64;
        idx++;
      }
      if (shift != 0) {
        result = (result << shift) | (data[idx] >>> 64 - shift);
      }
    }
    return result;
  }

  /**
   * Peek at the next n bits, and consume them if they match val.
   * @param n the number of bits to read
   * @param val the bits to compare against.
   * @return True if the bits match, and the cursor advances.
   * @throws ParseException if the reader has reached the end of the bit array.
   */
  public boolean tryRead(int n, long val) throws ParseException {
    long v = read(n);
    if (val == v) {
      return true;
    }
    shift -= n;
    if (shift < 0) {
      shift += 64;
      idx--;
    }
    return false;
  }

  @Override
  public String toString() {
    return "BitStreamIterator{"
        + "data=" + Arrays.toString(data)
        + ", bitLength=" + bitLength
        + ", idx=" + idx
        + ", shift=" + shift
        + '}';
  }

  @VisibleForTesting
  public int getBitLength() {
    return bitLength;
  }
}
