package org.apache.reef.inmemory.task;

import com.google.common.cache.CacheStats;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;

import java.io.IOException;

/**
 * Interface for InMemory Cache.
 */
public interface InMemoryCache {
  /**
   * Retrieves the content of a block with given blockId.
   * @param fileBlock Block identifier to read
   * @return The byte array containing the data of the block
   * @throws BlockLoadingException If the block is loading at the moment of trial
   * @throws BlockNotFoundException If the block does not exist in the cache
   */
  public byte[] get(BlockId fileBlock) throws BlockLoadingException, BlockNotFoundException;

  /**
   * Load data into the cache using the given block loader.
   * For efficiency reasons, implementations should assure that
   * multiple block loaders do not simultaneously load the same block.
   * @param fileBlock
   * @param blockLoader
   */
  public void load(BlockLoader blockLoader) throws IOException;

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
