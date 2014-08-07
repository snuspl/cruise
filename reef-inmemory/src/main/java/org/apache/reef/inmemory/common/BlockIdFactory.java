package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.task.BlockId;

import java.io.IOException;

/**
 * Factory that creates Block objects (using information from other Block objects)
 */
public interface BlockIdFactory<FsMetadata, FsBlockId extends BlockId> {

  /**
   * Create a new BlockId using information from BlockInfo.
   * BlockId implements equals() and hashCode(), which is suitable for use as a key.
   */
  public FsBlockId newBlockId(BlockInfo blockInfo);

  /**
   * Create a new BlockId from the FS-specific metadata.
   * BlockId implements equals() and hashCode(), which is suitable for use as a key.
   */
  public FsBlockId newBlockId(FsMetadata metadata) throws IOException;

  /**
   * Create a new BlockInfo from the FS-specific metadata.
   * BlockInfo is a Thrift data structure, used in communication with the Client.
   */
  public BlockInfo newBlockInfo(FsMetadata metadata) throws IOException;
}
