package com.pinterest.yuvi.tagstore;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A utils class to convert between ByteBuffer and RoaringBitmap.
 * Code copied from https://github.com/RoaringBitmap/RoaringBitmap/blob/c304f92b3c31b37cc2961053ed37d2591165bcba/roaringbitmap/src/test/java/org/roaringbitmap/buffer/TestMemoryMapping.java
 */
public class RoaringBitMapUtils {

  public static ByteBuffer toByteBuffer(MutableRoaringBitmap rb) {
    // we add tests
    ByteBuffer outbb = ByteBuffer.allocate(rb.serializedSizeInBytes());
    try {
      rb.serialize(new DataOutputStream(new ByteBufferBackedOutputStream(outbb)));
    } catch (IOException e) {
      e.printStackTrace();
    }
    //
    outbb.flip();
    outbb.order(ByteOrder.LITTLE_ENDIAN);
    return outbb;
  }

  public static ByteBuffer toByteBuffer(RoaringBitmap rb) {
    // we add tests
    ByteBuffer outbb = ByteBuffer.allocate(rb.serializedSizeInBytes());
    try {
      rb.serialize(new DataOutputStream(new ByteBufferBackedOutputStream(outbb)));
    } catch (IOException e) {
      e.printStackTrace();
    }
    //
    outbb.flip();
    outbb.order(ByteOrder.LITTLE_ENDIAN);
    return outbb;
  }

  public static MutableRoaringBitmap toMutableRoaringBitmap(ByteBuffer bb) {
    MutableRoaringBitmap rb = new MutableRoaringBitmap();
    try {
      rb.deserialize(new DataInputStream(new ByteBufferBackedInputStream(bb)));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return rb;
  }

  public static RoaringBitmap toRoaringBitmap(ByteBuffer bb) {
    RoaringBitmap rb = new RoaringBitmap();
    try {
      rb.deserialize(new DataInputStream(new ByteBufferBackedInputStream(bb)));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return rb;
  }

  static class ByteBufferBackedOutputStream extends OutputStream {

    ByteBuffer buf;

    ByteBufferBackedOutputStream(ByteBuffer buf) {
      this.buf = buf;
    }

    @Override
    public synchronized void write(byte[] bytes) throws IOException {
      buf.put(bytes);
    }

    @Override
    public synchronized void write(byte[] bytes, int off, int len) throws IOException {
      buf.put(bytes, off, len);
    }

    @Override
    public synchronized void write(int b) throws IOException {
      buf.put((byte) b);
    }
  }

  static class ByteBufferBackedInputStream extends InputStream {

    ByteBuffer buf;

    ByteBufferBackedInputStream(ByteBuffer buf) {
      this.buf = buf;
    }

    @Override
    public int available() throws IOException {
      return buf.remaining();
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    @Override
    public int read() throws IOException {
      if (!buf.hasRemaining()) {
        return -1;
      }
      return 0xFF & buf.get();
    }

    @Override
    public int read(byte[] bytes) throws IOException {
      int len = Math.min(bytes.length, buf.remaining());
      buf.get(bytes, 0, len);
      return len;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
      len = Math.min(len, buf.remaining());
      buf.get(bytes, off, len);
      return len;
    }

    @Override
    public long skip(long n) {
      int len = Math.min((int) n, buf.remaining());
      buf.position(buf.position() + (int) n);
      return len;
    }
  }
}
