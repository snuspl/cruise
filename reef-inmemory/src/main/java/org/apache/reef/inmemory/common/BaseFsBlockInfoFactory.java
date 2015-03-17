package org.apache.reef.inmemory.common;

import java.io.IOException;

/**
 * Factory that creates Block information to communicate with BaseFs.
 */
public interface BaseFsBlockInfoFactory<FsMetadata, FsBlockInfo> {
  /**
   * Create a new BlockId from the FS-specific metadata.
   * BlockId implements equals() and hashCode(), which is suitable for use as a key.
   * @param filePath Path of file, to be added to block ID
   * @param metadata FS-specific block metadata
   * @return Base-FS specific block ID information
   * @throws IOException
   */
  public FsBlockInfo newBlockInfo(String filePath, FsMetadata metadata) throws IOException;
}
