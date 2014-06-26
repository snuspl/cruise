package org.apache.reef.inmemory.cache;

import org.apache.reef.inmemory.fs.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.fs.exceptions.BlockNotFoundException;

/**
 * Interface for InMemory Cache.
 */
public interface InMemoryCache {
  public byte[] get(BlockId fileBlock) throws BlockLoadingException, BlockNotFoundException;
  public void put(BlockId fileBlock, byte[] data);

  void putPending(BlockId blockId);

  public void clear();
  public byte[] getReport();
}
