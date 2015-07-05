package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;

import java.io.IOException;
import java.util.List;

/**
 * Factory that creates Block metadata.
 */
public interface BlockMetaFactory<FsMetadata> {
  /**
   * Create a new BlockMeta from the FS-specific metadata.
   * BlockMeta is a Thrift data structure, used in communication with the Client.
   * @param fileId id of file, to be added to block info
   * @param metadata FS-specific block metadata
   * @return Block information stored as a Thrift data structure
   */
  BlockMeta newBlockMeta(long fileId, FsMetadata metadata);
}
