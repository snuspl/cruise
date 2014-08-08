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
   * @return the data loaded by BlockLoader
   * @throws BlockLoadingException
   */
  public byte[] getData() throws BlockLoadingException;
}