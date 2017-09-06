package com.pinterest.yuvi.metricstore;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import com.pinterest.yuvi.models.Point;
import com.pinterest.yuvi.bitstream.BitStreamIterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class VarBitTimeSeriesTest {

  private Instant startTime = Instant.parse("2016-12-03T10:01:02.00Z");
  private Random randGen = new Random();
  private final List<Double> testValues = Arrays.asList(1.0, -1.0, 0.0, -2.0, 2.0, 1000.01111111111,
      12.34566678888, 10000.0, 1023.23, 15.23, 10.34, Double.MAX_VALUE, Double.MIN_VALUE,
      Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTimeSeriesBlockHeaderOffset() {
    Instant expectedBlockHeaderInstant = Instant.parse("2016-12-03T10:00:00.00Z");

    // On the hour offset
    testBlockOffsetHeader(expectedBlockHeaderInstant,
        expectedBlockHeaderInstant.getEpochSecond(),
        0);

    // 90 seconds after offset.
    testBlockOffsetHeader(startTime, expectedBlockHeaderInstant.getEpochSecond(), 62);

    // After an hour and 90 seconds from offset
    testBlockOffsetHeader(startTime.plusSeconds(3600),
        expectedBlockHeaderInstant.getEpochSecond(),
        3662);

    // After 2 hour and 90 seconds from offset. Creates a new offset.
    testBlockOffsetHeader(startTime.plusSeconds(3600 * 2),
        expectedBlockHeaderInstant.plusSeconds(3600 * 2).getEpochSecond(),
        62);
  }

  @Test
  public void testMinTimestamp() {
    VarBitTimeSeries series = new VarBitTimeSeries();
    thrown.expect(IllegalArgumentException.class);
    series.append(-1, 1);
  }

  @Test
  public void testMaxTimestamp() {
    VarBitTimeSeries series = new VarBitTimeSeries();
    thrown.expect(IllegalArgumentException.class);
    series.append(-1, 1);
  }

  @Test
  public void testTimeEncoding() {
    long expectedBlockHeader = Instant.parse("2016-12-03T10:00:00.00Z").getEpochSecond();
    final long startSecond = startTime.getEpochSecond();

    // period of 62 seconds
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62).getEpochSecond(),
        expectedBlockHeader, 62, 47, 0);

    // period +1 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 + 1).getEpochSecond(),
        expectedBlockHeader, 62, 55, 64);

    // period -1 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 - 1).getEpochSecond(),
        expectedBlockHeader, 62, 55, 62);

    // period +64 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 + 64).getEpochSecond(),
        expectedBlockHeader, 62, 55, 127);

    // period -63 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 - 63).getEpochSecond(),
        expectedBlockHeader, 62, 55, 0);

    // period +65 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 + 65).getEpochSecond(),
        expectedBlockHeader, 62, 58, 320);

    // period -64 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 - 64).getEpochSecond(),
        expectedBlockHeader, 62, 58, 191);

    // period +66 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 + 66).getEpochSecond(),
        expectedBlockHeader, 62, 58, 321);

    // period -65 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 - 65).getEpochSecond(),
        expectedBlockHeader, 62, 58, 190);

    // period +256 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 + 256).getEpochSecond(),
        expectedBlockHeader, 62, 58, 511);

    // period -255 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 - 255).getEpochSecond(),
        expectedBlockHeader, 62, 58, 0);

    // period +255 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 + 255).getEpochSecond(),
        expectedBlockHeader, 62, 58, 510);

    // period -254 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 - 254).getEpochSecond(),
        expectedBlockHeader, 62, 58, 1);

    // period +257 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 + 257).getEpochSecond(),
        expectedBlockHeader, 62, 62, 2304);

    // period -256 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 - 256).getEpochSecond(),
        expectedBlockHeader, 62, 62, 1791);

    // period +258 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 + 258).getEpochSecond(),
        expectedBlockHeader, 62, 62, 2305);

    // period -257 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 - 257).getEpochSecond(),
        expectedBlockHeader, 62, 62, 1790);

    // period +2047 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 + 2047).getEpochSecond(),
        expectedBlockHeader, 62, 62, 4094);

    // period -2046 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 - 2046).getEpochSecond(),
        expectedBlockHeader, 62, 62, 1);

    // period +2048 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 + 2048).getEpochSecond(),
        expectedBlockHeader, 62, 62, 4095);

    // period -2047 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 - 2047).getEpochSecond(),
        expectedBlockHeader, 62, 62, 0);

    // period +2049 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 + 2049).getEpochSecond(),
        expectedBlockHeader, 62, 82, 2147485696L);

    // period -2048 second.
    addAndCheckFirstThreeValues(startSecond, startTime.plusSeconds(62 - 2048).getEpochSecond(),
        expectedBlockHeader, 62, 82, 2147481599L);
  }

  private void addAndCheckFirstThreeValues(long firstDataPoint, long secondDataPoint,
                                           long firstValue, long secondValue,
                                           int bitLength, long thirdValue) {
    VarBitTimeSeries series = new VarBitTimeSeries();
    series.append(firstDataPoint, 1);
    series.append(secondDataPoint, 1);
    assertEquals(2, series.getSize());
    BitStreamIterator it = series.getTimestamps().read();
    assertEquals(bitLength, it.getBitLength());
    checkFirstTwoValues(it, firstValue, secondValue);
    if (bitLength == 47) {
      assertEquals(it.read(1), 0);
    }
    if (bitLength == 55) {
      assertEquals(2, it.read(2));
      assertEquals(thirdValue, it.read(7));
    }
    if (bitLength == 58) {
      assertEquals(6, it.read(3));
      assertEquals(thirdValue, it.read(9));
    }
    if (bitLength == 62) {
      assertEquals(14, it.read(4));
      assertEquals(thirdValue, it.read(12));
    }
    if (bitLength == 82) {
      assertEquals(15, it.read(4));
      assertEquals(thirdValue, it.read(32));
    }
  }

  private void testBlockOffsetHeader(Instant startTime,
                                     long expectedFirstValue,
                                     long expectedSecondValue) {
    VarBitTimeSeries series = new VarBitTimeSeries();
    series.append(startTime.getEpochSecond(), 1);
    assertEquals(1, series.getSize());
    BitStreamIterator it = series.getTimestamps().read();
    assertEquals(46, it.getBitLength());
    checkFirstTwoValues(it, expectedFirstValue, expectedSecondValue);
  }

  private void checkFirstTwoValues(BitStreamIterator it, long firstValue, long secondValue) {
    assertEquals(firstValue, it.read(32));
    assertEquals(secondValue, it.read(14));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTimestampEncodingOverflow() {
    VarBitTimeSeries series = new VarBitTimeSeries();
    int i = 10;
    long ts1 = 1477678600L;
    long ts2 = 14776780L;
    long ts3 = 1477678580L;
    series.append(ts1, i);
    series.append(ts2, i);
    series.append(ts3, i);
  }

  @Test
  public void testSimpleTimestampInsertAndFetch() {
    checkSimpleTimestampInsertAndFetchAt(startTime);
    checkSimpleTimestampInsertAndFetchAt(Instant.parse("2016-12-03T10:00:00.00Z"));
    checkSimpleTimestampInsertAndFetchAt(Instant.parse("2016-12-31T23:59:59.00Z"));
    checkSimpleTimestampInsertAndFetchAt(Instant.parse("2016-12-31T23:59:00.00Z"));
  }

  private void checkSimpleTimestampInsertAndFetchAt(Instant startTime) {
    VarBitTimeSeries series = new VarBitTimeSeries();
    List<Long> times = new ArrayList<>();
    List<Long> offsets = Arrays.asList(0L, +1L, -1L, 64L, -63L, 65L, -64L, 66L, -65L, 256L, -255L,
        255L, -254L, 257L, -256L, 258L, -257L, 2047L, -2046L, 2048L, -2047L, 2049L, -2408L, 10000L,
        100000L, -10000L, 1000000L, -1000000L);
    long size = offsets.size();
    for (int i = 0; i < size; i++) {
      long time = startTime.plusSeconds((i * 62) + offsets.get(i)).getEpochSecond();
      times.add(time);
      series.append(time, i);
    }

    TimeSeriesIterator dr = series.read();
    List<Point> points = dr.getPoints();
    assertEquals(size, points.size());
    for (int i = 0; i < size; i++) {
      assertEquals(points.get(i).getTs(), times.get(i).longValue());
    }
  }

  @Test
  public void testFirstValueEncoding() {
    testValues.stream().forEach(value -> checkFirstValueEncoding(value));
  }

  private void checkFirstValueEncoding(double testValue) {
    VarBitTimeSeries series = new VarBitTimeSeries();
    series.append(startTime.getEpochSecond(), testValue);

    assertEquals(1, series.getSize());

    BitStreamIterator it = series.getValues().read();
    assertEquals(64, it.getBitLength());
    assertEquals(testValue, Double.longBitsToDouble(it.read(64)));
  }

  @Test
  public void testFirstTwoValuesEncoding() {
    // Check that 0 bit is set when same value is encoded twice.
    testValues.stream().forEach(value ->
        checkFirstTwoIdenticalValuesEncoding(value, value, 65, value, 0.0));

    // Test cases with b11 encoding
    checkFirstTwoValuesWithNonOverlappingZeros(12, 24, 78, 12, 11, 52, 1);
    checkFirstTwoValuesWithNonOverlappingZeros(24, 15, 81, 24, 11, 49, 11);
    checkFirstTwoValuesWithNonOverlappingZeros(15, 12, 79, 15, 13, 49, 3);
    checkFirstTwoValuesWithNonOverlappingZeros(12, 35, 85, 12, 9, 47, 211);

    checkFirstTwoValuesWithNonOverlappingZeros(15.5, 14.0625, 82, 15.5, 14, 45, 25);
    checkFirstTwoValuesWithNonOverlappingZeros(14.0625, 3.25, 86, 14.0625, 10, 45, 305);
    checkFirstTwoValuesWithNonOverlappingZeros(3.25, 8.625, 85, 3.25, 10, 46, 173);
    checkFirstTwoValuesWithNonOverlappingZeros(8.625, 13.1, 129, 8.625, 12, 0, 3222888483337011L);

    // TODO: b10 path is tested in other tests. So, leaving detailed tests as a TODO.
  }

  private void checkFirstTwoIdenticalValuesEncoding(double firstValue, double secondValue,
                                                    int expectedBitLength,
                                                    double expectedFirstValue,
                                                    double expectedSecondValue) {

    VarBitTimeSeries series = new VarBitTimeSeries();
    series.append(startTime.getEpochSecond(), firstValue);
    series.append(startTime.plusSeconds(60).getEpochSecond(), secondValue);

    assertEquals(2, series.getSize());

    BitStreamIterator it = series.getValues().read();
    assertEquals(expectedBitLength, it.getBitLength());
    assertEquals(expectedFirstValue, Double.longBitsToDouble(it.read(64)));
    assertEquals(expectedSecondValue, Double.longBitsToDouble(it.read(1)));
  }

  private void checkFirstTwoValuesWithNonOverlappingZeros(double firstValue, double secondValue,
                                                          int expectedBitLength,
                                                          double expectedFirstValue,
                                                          int expectedLeadingZeros,
                                                          int expectedTrailingZeros,
                                                          long expectedXorValue) {

    VarBitTimeSeries series = new VarBitTimeSeries();
    series.append(startTime.getEpochSecond(), firstValue);
    series.append(startTime.plusSeconds(60).getEpochSecond(), secondValue);

    assertEquals(2, series.getSize());

    BitStreamIterator it = series.getValues().read();
    assertEquals(expectedBitLength, it.getBitLength());
    assertEquals(expectedFirstValue, Double.longBitsToDouble(it.read(64)));
    assertEquals(3, it.read(2));
    assertEquals(expectedLeadingZeros, it.read(5));
    assertEquals(expectedTrailingZeros, it.read(6));
    assertEquals(expectedXorValue, it.read(64 - expectedLeadingZeros - expectedTrailingZeros));
  }

  @Test
  public void testEmptyPoints() {
    VarBitTimeSeries series = new VarBitTimeSeries();
    TimeSeriesIterator dr = series.read();
    List<Point> points = dr.getPoints();
    assertTrue(points.isEmpty());
  }

  @Test
  public void testSimplePoints() {
    VarBitTimeSeries series = new VarBitTimeSeries();
    List<Double> nums = Arrays.asList(0.0, 1.0, -1.0);
    final long epochSecond = startTime.getEpochSecond();
    nums.stream().forEach(n -> series.append(epochSecond, n));

    TimeSeriesIterator dr = series.read();
    List<Point> points = dr.getPoints();
    assertEquals(3, points.size());
    points.stream().forEach(p -> assertTrue(nums.contains(p.getVal())));
  }

  @Test
  public void testAddZeros() {
    VarBitTimeSeries series = new VarBitTimeSeries();
    for (long i = 0; i < 1000; i++) {
      series.append(startTime.getEpochSecond(), 0);
    }

    TimeSeriesIterator dr = series.read();
    List<Point> points = dr.getPoints();
    assertEquals(1000, points.size());
    assertEquals(0, sumPoints(points));
    points.stream().forEach(p -> assertEquals(0.0, p.getVal()));
  }

  @Test
  public void testAddOnes() {
    VarBitTimeSeries series = new VarBitTimeSeries();
    for (long i = 0; i < 1000; i++) {
      series.append(startTime.getEpochSecond(), 1);
    }

    TimeSeriesIterator dr = series.read();
    List<Point> points = dr.getPoints();
    assertEquals(1000, points.size());
    assertEquals(1000, sumPoints(points));
    points.stream().forEach(p -> assertEquals(1.0, p.getVal()));
  }

  @Test
  public void testNegativeNumbers() {
    VarBitTimeSeries series = new VarBitTimeSeries();
    for (long i = 0; i < 1000; i++) {
      series.append(startTime.getEpochSecond(), -1);
    }

    TimeSeriesIterator dr = series.read();
    List<Point> points = dr.getPoints();
    assertEquals(1000, points.size());
    assertEquals(-1000, sumPoints(points));
    points.stream().forEach(p -> assertEquals(-1.0, p.getVal()));
  }

  @Test
  public void testAddPoints() {
    VarBitTimeSeries series = new VarBitTimeSeries();
    for (long i = 0; i < 1000; i++) {
      series.append(startTime.getEpochSecond(), (double) i);
    }

    TimeSeriesIterator dr = series.read();
    List<Point> points = dr.getPoints();
    assertEquals(1000, points.size());
    assertEquals((999 * 1000) / 2, sumPoints(points));
  }

  @Test
  public void testFewFloats() {
    List<String> nums = Arrays.asList("40869ca8d0a9e6d6", "408483f2c54930c8", "4089b9fe25ff3c29");
    insertAndReadHexNums(nums, startTime.getEpochSecond());
  }

  @Test
  public void testLeadingZeros() {
    List<String> nums = Arrays.asList("0", "0000000000000001", "4089b9fe25ff3c29");
    insertAndReadHexNums(nums, startTime.getEpochSecond());
  }

  @Test
  public void testTrailingZeros() {
    List<String> nums = Arrays.asList("0", "9000000000000000");
    insertAndReadHexNums(nums, startTime.getEpochSecond());
  }

  @Test
  public void testTralingAndLeadingZeros() {
    List<String> nums1 = Arrays.asList("40869ca8d0a9e6d6", "408483f2c54930c8",
        "4089b9fe25ff3c29", "fff0000000000000");
    insertAndReadHexNums(nums1, startTime.getEpochSecond());

    List<String> nums2 = Arrays.asList("0", "9000000000000000", "4089b9fe25ff3c29");
    insertAndReadHexNums(nums2, startTime.getEpochSecond());

    List<Double> nansAndInf = Arrays.asList(3.0, Double.NaN, Double.POSITIVE_INFINITY, 4.0,
        Double.MIN_VALUE, 3.654, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, -45.67,
        Double.POSITIVE_INFINITY, Double.MAX_VALUE, 0.0, Double.MIN_NORMAL, -1.0);
    insertAndReadNums(nansAndInf, startTime.getEpochSecond());
  }

  private void insertAndReadHexNums(List<String> nums, long startSecond) {
    List<Double> numList = nums.stream()
        .map(n -> Double.longBitsToDouble(new BigInteger(n, 16).longValue()))
        .collect(Collectors.toList());

    insertAndReadNums(numList, startSecond);
  }

  private void insertAndReadNums(List<Double> numList, long startSecond) {
    VarBitTimeSeries series = new VarBitTimeSeries();
    List<Point> inputPoints = numList.stream()
        .map(val -> new Point(startSecond + 60 * randGen.nextInt(10), val))
        .collect(Collectors.toList());

    List<Long> times = inputPoints.stream().map(p -> p.getTs()).collect(Collectors.toList());

    inputPoints.forEach(p -> series.append(p.getTs(), p.getVal()));

    TimeSeriesIterator dr = series.read();
    List<Point> points = dr.getPoints();
    assertEquals(numList.size(), points.size());
    points.stream().forEach(p -> assertTrue(numList.contains(p.getVal())));
    points.stream().forEach(p -> assertTrue(times.contains(p.getTs())));
    assertEquals(times.stream().mapToLong(l -> l).sum(),
        points.stream().mapToLong(p -> p.getTs()).sum());
  }

  private int sumPoints(List<Point> points) {
    return points.stream().mapToInt(p -> (int) p.getVal()).sum();
  }
}