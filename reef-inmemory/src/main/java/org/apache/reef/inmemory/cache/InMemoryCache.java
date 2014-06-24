package org.apache.reef.inmemory.cache;

import java.nio.ByteBuffer;

/**
 * Interface for InMemory Cache.
 */
public interface InMemoryCache {
  public byte[] get(BlockId fileBlock);
  public void put(BlockId fileBlock, byte[] data);
  public void clear();
  public byte[] getReport();
}
