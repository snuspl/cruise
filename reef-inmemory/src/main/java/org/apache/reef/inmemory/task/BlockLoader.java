package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;

import java.io.IOException;

/**
 * An interface to load a block from the underlying file systems
 */
public interface BlockLoader {
  /**
   * Load a block assigned to this Loader.
   * This method will only be called once per BlockLoader.
   * @throws IOException
   */
  public void loadBlock() throws IOException;

  /**
   * @return Block Identifier
   */
  public BlockId getBlockId();

  /**
   * @return Whether block is configured for pinning
   */
  public boolean isPinned();

  /**
   * @param index Index of the chunk to load
   * @return Part of the data loaded by BlockLoader
   * @throws BlockLoadingException If the chunk of index has not been loaded yet
   */
  public byte[] getData(int index) throws BlockLoadingException;

  /**
   * @return Size of buffer used in block loading
   */
  public int getBufferSize();
}