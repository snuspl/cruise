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
   * Create a new BlockMeta using BlockId and the block locations
   * BlockMeta is a Thrift data structure, used in communication with the Client.
   * @param blockId Block Identifier
   * @param blockSize Size of the block
   * @param nodes The cache nodes owning the block
   * @return Block information stored as a Thrift data structure
   * @throws IOException
   */
  public BlockMeta newBlockMeta(BlockId blockId, long blockSize, List<NodeInfo> nodes);

  /**
   * Create a new BlockMeta from the FS-specific metadata.
   * BlockMeta is a Thrift data structure, used in communication with the Client.
   * @param fileId id of file, to be added to block info
   * @param metadata FS-specific block metadata
   * @return Block information stored as a Thrift data structure
   * @throws IOException
   */
  public BlockMeta newBlockMeta(long fileId, FsMetadata metadata) throws IOException;
}
