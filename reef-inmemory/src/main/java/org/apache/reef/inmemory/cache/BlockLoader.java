package org.apache.reef.inmemory.cache;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An interface to load a block from the underlying file systems
 */
public interface BlockLoader {
  public ByteBuffer loadBlock() throws IOException;
}