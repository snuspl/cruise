package org.apache.reef.inmemory.cache;

import java.io.IOException;

/**
 * An interface to load a block from the underlying file systems
 */
public interface BlockLoader {
  /**
   * Load a block assigned to this Loader.
   * @return byteBuffer holds the data it loaded.
   * @throws IOException
   */
  public byte[] loadBlock() throws IOException;

  /**
   * @return Block Identifier
   */
  public BlockId getBlockId();
}