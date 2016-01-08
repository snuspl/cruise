package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;

import java.io.IOException;

/**
 * An interface to load a block from the base file systems.
 */
public interface BlockLoader {
  /**
   * Load a block assigned to this Loader.
   * This method will only be called once per BlockLoader.
   * @throws IOException
   */
  void loadBlock() throws IOException;

  /**
   * @return Block Identifier
   */
  BlockId getBlockId();

  /**
   * @return Size of the block
   */
  long getBlockSize();

  /**
   * @return Whether block is configured for pinning
   */
  boolean isPinned();

  /**
   * @param index Index of the chunk to get
   * @return Part of the data loaded by BlockLoader
   * @throws BlockLoadingException If the chunk of index has not been loaded yet
   */
  byte[] getData(int index) throws BlockLoadingException;
}