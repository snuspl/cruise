package org.apache.reef.inmemory.cache;

import java.nio.ByteBuffer;

/**
 * Interface for InMemory Cache.
 */
public interface InMemoryCache {
  public ByteBuffer get(BlockId fileBlock);
  public void put(BlockId fileBlock, ByteBuffer buffer);
  public void clear();
  public byte[] getReport();
}
