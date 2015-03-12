package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.common.entity.BlockMeta;

import java.io.IOException;

/**
 * Factory that creates Block information to communicate with BaseFs.
 */
public interface BaseFsBlockMetaFactory<FsMetadata, FsBlockMeta> {
  public FsBlockMeta newBlockId(BlockMeta blockMeta);

  /**
   * Create a new BlockId from the FS-specific metadata.
   * BlockId implements equals() and hashCode(), which is suitable for use as a key.
   * @param filePath Path of file, to be added to block ID
   * @param metadata FS-specific block metadata
   * @return Base-FS specific block ID information
   * @throws IOException
   */
  public FsBlockMeta newBlockId(String filePath, FsMetadata metadata) throws IOException;

  /**
   * Create a new BlockMeta from the FS-specific metadata.
   * BlockMeta is a Thrift data structure, used in communication with the Client.
   * @param filePath Path of file, to be added to block info
   * @param metadata FS-specific block metadata
   * @return Block information stored as a Thrift data structure
   * @throws IOException
   */
  public BlockMeta newBlockMeta(String filePath, FsMetadata metadata) throws IOException;
}
