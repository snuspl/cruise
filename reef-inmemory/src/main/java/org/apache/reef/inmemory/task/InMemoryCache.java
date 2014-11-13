package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.exceptions.BlockNotWritableException;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for InMemory Cache.
 */
public interface InMemoryCache {

  /**
   * Retrieves the content of a block with given blockId.
   * @param blockId Block identifier to read
   * @param index Chunk index inside block
   * @return The byte array containing the data of the block
   * @throws BlockLoadingException If the block is loading at the moment of trial
   * @throws BlockNotFoundException If the block does not exist in the cache
   */
  public byte[] get(BlockId blockId, int index) throws BlockLoadingException, BlockNotFoundException;

  /**
   * Write the data into the block specified by blockId
   * @param blockId Block identifier to write
   * @param offset Offset from the start of Block
   * @param data Data to write
   * @param isLastPacket The data is the last part of this block
   * @throws BlockNotFoundException If the block does not exist in the cache for the blockId
   * @throws BlockNotWritableException If the block is not supposed to write data into
   * @throws IOException If it fails while writing the data
   */
  public void write(BlockId blockId, long offset, ByteBuffer data, boolean isLastPacket) throws BlockNotFoundException, BlockNotWritableException, IOException;

  /**
   * Load data into the cache using the given block loader.
   * For efficiency reasons, implementations should assure that
   * multiple block loaders do not simultaneously load the same block.
   * @param loader The block loader contains FS-specific information, as well as BlockId and pin information
   * @throws IOException If block loading fails
   */
  public void load(BlockLoader loader) throws IOException;

  /**
   * Insert an entry for a blank block into cache before filling data.
   */
  public void prepareToLoad(BlockLoader loader) throws IOException, BlockNotFoundException;

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
}
