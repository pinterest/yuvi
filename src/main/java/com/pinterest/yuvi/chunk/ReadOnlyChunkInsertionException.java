package com.pinterest.yuvi.chunk;

/**
 * An exception thrown by chunk manager when we try to insert a metric into a read only chunk.
 */
public class ReadOnlyChunkInsertionException extends Exception {

  public ReadOnlyChunkInsertionException(String message) {
    super(message);
  }
}
