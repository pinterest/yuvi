package com.pinterest.yuvi.bitstream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.pinterest.yuvi.bitstream.BitStream;
import com.pinterest.yuvi.bitstream.BitStreamIterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;

public class BitStreamTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testEncode() {
    BitStream stream = new BitStream(1);
    stream.write(10, 10);
    stream.write(4, 4);
    assertEquals(1, stream.getCapacity());
    assertEquals(0, stream.getIndex());
    assertEquals(14, stream.getShift());
    assertEquals(1, stream.getData().length);
    assertEquals(stream.getCapacity(), stream.getData().length);

    BitStreamIterator it = stream.read();
    assertTrue(it.tryRead(10, 10));
    assertTrue(it.tryRead(4, 4));

    stream.write(63, 40);
    assertEquals(2, stream.getCapacity());
    assertEquals(1, stream.getIndex());
    assertEquals(13, stream.getShift());
    assertEquals(2, stream.getData().length);
    assertEquals(stream.getCapacity(), stream.getData().length);

    BitStreamIterator it2 = stream.read();
    assertTrue(it2.tryRead(10, 10));
    assertTrue(it2.tryRead(4, 4));
    assertTrue(it2.tryRead(63, 40));

    stream.write(62, 60);
    assertEquals(4, stream.getCapacity());
    assertEquals(2, stream.getIndex());
    assertEquals(11, stream.getShift());
    assertEquals(4, stream.getData().length);
    assertEquals(stream.getCapacity(), stream.getData().length);

    BitStreamIterator it3 = stream.read();
    assertTrue(it3.tryRead(10, 10));
    assertTrue(it3.tryRead(4, 4));
    assertTrue(it3.tryRead(63, 40));
    assertTrue(it3.tryRead(62, 60));
  }

  @Test
  public void testDefaultInitialization() {
    BitStream stream = new BitStream();
    assertEquals(BitStream.DEFAULT_INITIAL_CAPACITY, stream.getCapacity());
    assertEquals(0, stream.getIndex());
    assertEquals(0, stream.getShift());
    assertEquals(16, stream.getData().length);
    assertEquals(16, stream.getCapacity());
    assertEquals(stream.getCapacity(), stream.getData().length);
  }

  @Test
  public void testSerializeDeserialize() throws Exception {
    BitStream stream = new BitStream( 2);
    stream.write(10, 10);
    stream.write(63, 4);
    stream.write(60, 61);
    assertEquals(4, stream.getCapacity());
    assertEquals(2, stream.getIndex());
    assertEquals(5, stream.getShift());
    assertEquals(4, stream.getData().length);
    assertEquals(stream.getCapacity(), stream.getData().length);
    assertEquals(29, stream.getSerializedByteSize());

    ByteBuffer buffer = ByteBuffer.allocate(stream.getSerializedByteSize());
    stream.serialize(buffer);
    buffer.flip();

    BitStream deserializedStream = BitStream.deserialize(buffer);
    BitStreamIterator it = deserializedStream.read();
    assertEquals(3, deserializedStream.getCapacity());
    assertEquals(2, deserializedStream.getIndex());
    assertEquals(5, deserializedStream.getShift());
    assertEquals(3, deserializedStream.getData().length);
    assertEquals(deserializedStream.getCapacity(), deserializedStream.getData().length);
    assertEquals(29, deserializedStream.getSerializedByteSize());
    assertTrue(it.tryRead(10, 10));
    assertTrue(it.tryRead(63, 4));
    assertTrue(it.tryRead(60, 61));

    // Write to deserialized stream and ensure that the resultant stream is a valid BitStream.
    deserializedStream.write(2, 1);
    assertEquals(29, deserializedStream.getSerializedByteSize());

    ByteBuffer buffer2 = ByteBuffer.allocate(deserializedStream.getSerializedByteSize());
    deserializedStream.serialize(buffer2);
    buffer2.flip();

    BitStream deserializedStream2 = BitStream.deserialize(buffer2);
    assertEquals(3, deserializedStream2.getCapacity());
    assertEquals(2, deserializedStream2.getIndex());
    assertEquals(7, deserializedStream2.getShift());
    assertEquals(3, deserializedStream2.getData().length);
    assertEquals(deserializedStream2.getCapacity(), deserializedStream2.getData().length);
    BitStreamIterator it2 = deserializedStream2.read();
    assertTrue(it2.tryRead(10, 10));
    assertTrue(it2.tryRead(63, 4));
    assertTrue(it2.tryRead(60, 61));
    assertTrue(it2.tryRead(2, 1));

    // Write a long value so we can test capacity doubling.
    deserializedStream2.write(63, 65);
    assertEquals(6, deserializedStream2.getCapacity());
    assertEquals(3, deserializedStream2.getIndex());
    assertEquals(6, deserializedStream2.getShift());
    assertEquals(6, deserializedStream2.getData().length);
    assertEquals(deserializedStream2.getCapacity(), deserializedStream2.getData().length);
    assertEquals(37, deserializedStream2.getSerializedByteSize());

    ByteBuffer buffer3 = ByteBuffer.allocate(deserializedStream2.getSerializedByteSize());
    deserializedStream2.serialize(buffer3);
    buffer3.flip();

    BitStream deserializedStream3 = BitStream.deserialize(buffer3);
    assertEquals(4, deserializedStream3.getCapacity());
    assertEquals(3, deserializedStream3.getIndex());
    assertEquals(6, deserializedStream3.getShift());
    assertEquals(4, deserializedStream3.getData().length);
    assertEquals(deserializedStream3.getCapacity(), deserializedStream3.getData().length);
    assertEquals(37, deserializedStream3.getSerializedByteSize());
    BitStreamIterator it3 = deserializedStream3.read();
    assertTrue(it3.tryRead(10, 10));
    assertTrue(it3.tryRead(63, 4));
    assertTrue(it3.tryRead(60, 61));
    assertTrue(it3.tryRead(2, 1));
    assertTrue(it3.tryRead(63, 65));
  }

  @Test
  public void testSerializeDeserialzeWithLazyDataAllocation() throws Exception {
    BitStream stream = new BitStream(2);
    stream.write(1, 0);
    stream.write(63, 10);
    stream.write(64, 20);
    assertEquals(2, stream.getCapacity());
    assertEquals(2, stream.getIndex());  // Index is 2, even though there is no data[2]
    assertEquals(0, stream.getShift());
    assertEquals(2, stream.getData().length);
    assertEquals(stream.getCapacity(), stream.getData().length);
    assertEquals(21, stream.getSerializedByteSize());

    ByteBuffer buffer = ByteBuffer.allocate(stream.getSerializedByteSize());
    stream.serialize(buffer);
    buffer.flip();

    BitStream deserializedStream = BitStream.deserialize(buffer);
    BitStreamIterator it = deserializedStream.read();
    assertEquals(2, deserializedStream.getCapacity());
    assertEquals(2, deserializedStream.getIndex());
    assertEquals(0, deserializedStream.getShift());
    assertEquals(2, deserializedStream.getData().length);
    assertEquals(deserializedStream.getCapacity(), deserializedStream.getData().length);
    assertEquals(21, deserializedStream.getSerializedByteSize());
    assertTrue(it.tryRead(1, 0));
    assertTrue(it.tryRead(63, 10));
    assertTrue(it.tryRead(64, 20));

    deserializedStream.write(1, 1);
    BitStreamIterator it1 = deserializedStream.read();
    assertTrue(it1.tryRead(1, 0));
    assertTrue(it1.tryRead(63, 10));
    assertTrue(it1.tryRead(64, 20));
    assertTrue(it1.tryRead(1, 1));
    assertEquals(29, deserializedStream.getSerializedByteSize());

    ByteBuffer buffer2 = ByteBuffer.allocate(deserializedStream.getSerializedByteSize());
    deserializedStream.serialize(buffer2);
    buffer2.flip();

    BitStream deserializedStream2 = BitStream.deserialize(buffer2);
    assertEquals(3, deserializedStream2.getCapacity());
    assertEquals(2, deserializedStream2.getIndex());
    assertEquals(1, deserializedStream2.getShift());
    assertEquals(3, deserializedStream2.getData().length);
    assertEquals(deserializedStream2.getCapacity(), deserializedStream2.getData().length);
    BitStreamIterator it2 = deserializedStream2.read();
    assertTrue(it2.tryRead(1, 0));
    assertTrue(it2.tryRead(63, 10));
    assertTrue(it2.tryRead(64, 20));
    assertTrue(it2.tryRead(1, 1));
  }

  @Test
  public void testNegativeBitEncoding() {
    thrown.expect(IllegalArgumentException.class);
    new BitStream( 2).write(-1, 10);
  }

  @Test
  public void testZeroBitEncoding() {
    thrown.expect(IllegalArgumentException.class);
    new BitStream( 2).write(0, 10);
  }

  @Test
  public void test9ByteEncoding() {
    thrown.expect(IllegalArgumentException.class);
    new BitStream( 2).write(65, 10);
  }
}
