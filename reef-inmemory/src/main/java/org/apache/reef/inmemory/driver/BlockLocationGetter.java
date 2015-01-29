package org.apache.reef.inmemory.driver;

import java.io.IOException;

/**
 * Get block locations from BaseFS. Each implementation can specify the representation of block locations
 * and file identifier.
 */
public interface BlockLocationGetter<FSFileId, FSBlockLocations> {
  /**
   * Returns the block locations of file.
   * @param id Identifier to distinguish a unique file.
   * @return Block locations of the file identified by {@code id}.
   * @throws IOException
   */
  public FSBlockLocations getBlockLocations(FSFileId id) throws IOException;
}
