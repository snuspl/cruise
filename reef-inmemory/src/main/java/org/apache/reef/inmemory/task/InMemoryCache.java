package org.apache.reef.inmemory.task;

import com.google.common.cache.CacheStats;
import org.apache.reef.inmemory.driver.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.driver.exceptions.BlockNotFoundException;

import java.nio.ByteBuffer;

/**
 * Interface for InMemory Cache.
 */
public interface InMemoryCache {
  /**
   * Retrives the content of a block with given blockId
   * @param fileBlock Block identifier to read
   * @return The byte array containing the data of the block
   * @throws BlockLoadingException If the block is loading at the moment of trial
   * @throws BlockNotFoundException If the block does not exist in the cache
   */
  public byte[] get(BlockId fileBlock) throws BlockLoadingException, BlockNotFoundException;

  /**
   * Read the content of a block into ByteBuffer with given offset
   * @param fileBlock Block identifier to read
   * @param out The ByteBuffer to hold the data
   * @param offset The offset inside the block
   * @throws BlockLoadingException If the block is loading at the moment of trial
   * @throws BlockNotFoundException If the block does not exist in the cache
   */
  public void read(BlockId fileBlock, ByteBuffer out, long offset)
          throws BlockLoadingException, BlockNotFoundException;

  /**
   * Put data into the cache when loading a block is completed
   * @param fileBlock Block identifier to read
   * @param data Data to put into the cache
   */
  public void put(BlockId fileBlock, byte[] data);

  /**
   * Mark the block is in pending state (loading the data)
   * @param blockId Block identifier to read
   */
  void putPending(BlockId blockId);

  /**
   * Clears the cache
   */
  public void clear();

  /**
   * Retrieve information about the cache status
   * @return The status of Cache
   */
  public CacheStats getReport(); // TODO: Dependency on Google task implementation -- change to new class?
}
