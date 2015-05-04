package org.apache.reef.inmemory.common;

import java.io.IOException;

/**
 * Factory that creates Block information to communicate with BaseFs.
 */
public interface BaseFsBlockInfoFactory<FsMetadata, FsBlockInfo> {
  /**
   * Create a new BlockInfo from the FS-specific metadata.
   * @param filePath Path of file, to be added to block ID
   * @param metadata FS-specific block metadata
   * @return Base-FS specific block ID information
   * @throws IOException if the block token of FsMetadata cannot be encoded as a url string
   */
  public FsBlockInfo newBlockInfo(String filePath, FsMetadata metadata) throws IOException;
}
