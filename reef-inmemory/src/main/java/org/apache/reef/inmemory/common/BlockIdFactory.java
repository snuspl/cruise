package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.task.BlockId;

import java.io.IOException;
import java.util.List;

/**
 * Factory that creates Block objects (using information from other Block objects)
 */
public interface BlockIdFactory<FsMetadata, FsBlockId extends BlockId> {

  /**
   * Create a new BlockId using information from BlockInfo.
   * BlockId implements equals() and hashCode(), which is suitable for use as a key.
   * @param blockInfo Block information stored as a Thrift data structure
   * @return Base-FS specific block ID information
   */
  public FsBlockId newBlockId(BlockInfo blockInfo);

  /**
   * Create a new BlockId from the FS-specific metadata.
   * BlockId implements equals() and hashCode(), which is suitable for use as a key.
   * @param filePath Path of file, to be added to block ID
   * @param metadata FS-specific block metadata
   * @return Base-FS specific block ID information
   * @throws IOException
   */
  public FsBlockId newBlockId(String filePath, FsMetadata metadata) throws IOException;

  /**
   * Create a new BlockId using (path, offset, blockSize) which is unique per block
   * BlockId implements equals() and hashCode(), which is suitable for use as a key.
   * @param filePath Path of file, to be added to block info
   * @param offset Offset from the start of file
   * @param blockSize Size of the block
   * @return General-purpose block ID information
   */
  public FsBlockId newBlockId(String filePath, long offset, long blockSize);

  /**
   * Create a new BlockInfo from the FS-specific metadata.
   * BlockInfo is a Thrift data structure, used in communication with the Client.
   * @param filePath Path of file, to be added to block info
   * @param metadata FS-specific block metadata
   * @return Block information stored as a Thrift data structure
   * @throws IOException
   */
  public BlockInfo newBlockInfo(String filePath, FsMetadata metadata) throws IOException;

  /**
   * Create a new BlockInfo using BlockId and the block locations
   * BlockInfo is a Thrift data structure, used in communication with the Client.
   * @param blockId Block Identifier
   * @param nodes The cache nodes owning the block
   * @return Block information stored as a Thrift data structure
   * @throws IOException
   */
  public BlockInfo newBlockInfo(FsBlockId blockId, List<NodeInfo> nodes);
}
