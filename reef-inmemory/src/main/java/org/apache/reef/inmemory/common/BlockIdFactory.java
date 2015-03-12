package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.task.BlockId;

import java.io.IOException;
import java.util.List;

/**
 * Factory that creates Block objects (using information from other Block objects)
 */
public interface BlockIdFactory<FsBlockId extends BlockId> {

  /**
   * Create a new BlockId using information from BlockMeta.
   * BlockId implements equals() and hashCode(), which is suitable for use as a key.
   * @param blockMeta Block information stored as a Thrift data structure
   * @return Base-FS specific block ID information
   */
  public FsBlockId newBlockId(BlockMeta blockMeta);

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
   * Create a new BlockMeta using BlockId and the block locations
   * BlockMeta is a Thrift data structure, used in communication with the Client.
   * @param blockId Block Identifier
   * @param nodes The cache nodes owning the block
   * @return Block information stored as a Thrift data structure
   * @throws IOException
   */
  public BlockMeta newBlockMeta(FsBlockId blockId, List<NodeInfo> nodes);
}
