package org.apache.reef.inmemory.common.hdfs;

import org.apache.reef.inmemory.common.BlockIdFactory;
import org.apache.reef.inmemory.common.BlockIdImpl;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;

import javax.inject.Inject;
import java.util.List;

public final class HdfsBlockIdFactory implements BlockIdFactory<BlockIdImpl> {

  @Inject
  public HdfsBlockIdFactory() {
  }

  /**
   * Create a new HdfsBlockId using information from BlockMeta
   */
  @Override
  public BlockIdImpl newBlockId(final BlockMeta blockMeta) {
    return new BlockIdImpl(
            blockMeta.getFilePath(),
            blockMeta.getOffSet(),
            blockMeta.getLength());
  }

  /**
   * Assign a BlockId to create a new block when writing data directly to Surf.
   * Fields unknown at the creation time are marked as -1 or null.
   * TODO These fields will be removed once we loose the tight dependencies on HDFS
   */
  @Override
  public BlockIdImpl newBlockId(String filePath, long offset, long blockSize) {
    return new BlockIdImpl(filePath, offset, blockSize);
  }

  /**
   * Create a new BlockMeta with the assigned BlockId and allocated nodes.
   * Fields unknown at the creation time are marked as -1 or null.
   */
  @Override
  public BlockMeta newBlockMeta(BlockIdImpl blockId, List<NodeInfo> nodes) {
    BlockMeta blockMeta = new BlockMeta();

    blockMeta.setFilePath(blockId.getFilePath());
    blockMeta.setOffSet(blockId.getOffset());
    blockMeta.setLength(blockId.getBlockSize());
    blockMeta.setLocations(nodes);
    return blockMeta;
  }
}
