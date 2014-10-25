package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.replication.Action;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for InMemory Cache.
 */
public interface InMemoryCache {

  /**
   * Retrieves the content of a block with given blockId.
   * @param fileBlock Block identifier to read
   * @param index Chunk index inside block
   * @return The byte array containing the data of the block
   * @throws BlockLoadingException If the block is loading at the moment of trial
   * @throws BlockNotFoundException If the block does not exist in the cache
   */
  public byte[] get(BlockId fileBlock, int index) throws BlockLoadingException, BlockNotFoundException;

  /**
   * Load data into the cache using the given block loader.
   * For efficiency reasons, implementations should assure that
   * multiple block loaders do not simultaneously load the same block.
   * @param loader The block loader contains FS-specific information, as well as BlockId and pin information
   * @throws IOException If block loading fails
   */
  public void load(BlockLoader loader) throws IOException;

  /**
   * @return Length of buffer loading data from Underlying File Systemss
   */
  public int getLoadingBufferSize();

  /**
   * Clears the cache
   */
  public void clear();

  /**
   * Retrieve information about the cache status
   * @return The status of Cache
   */
  public CacheStatistics getStatistics();

  /**
   * Pull the cache updates. The returned updates are removed.
   * @return The latest updates from the cache Task
   */
  public CacheUpdates pullUpdates();

  /**
   * Write the data
   * @param blockId Identifier of the block to write data
   * @param offset Offset of data from the start of the block
   * @param buf Data buffered as much as buffer size
   */
  public void write(BlockId blockId, long offset, byte[] buf) throws IOException;

  /**
   * Allocate a block before writing the data
   * @param blockId
   * @param action
   */
  public void allocate(BlockId blockId, Action action) throws IOException;
}
