package org.apache.reef.inmemory.common.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.BlockMetaFactory;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;

public final class HdfsBlockMetaFactory implements BlockMetaFactory<LocatedBlock> {

  @Inject
  public HdfsBlockMetaFactory() {
  }

  /**
   * Create a new BlockMeta with the assigned BlockId and allocated nodes.
   * Fields unknown at the creation time are marked as -1 or null.
   */
  @Override
  public BlockMeta newBlockMeta(final BlockId blockId, final long blockSize, final List<NodeInfo> nodes) {
    BlockMeta blockMeta = new BlockMeta();
    blockMeta.setFileId(blockId.getFileId());
    blockMeta.setOffSet(blockId.getOffset());
    blockMeta.setLength(blockSize);
    blockMeta.setLocations(nodes);
    return blockMeta;
  }

  /**
   * Create a new BlockMeta using identifying information from LocatedBlock. Does /not/ copy
   * location information (as it is not identifying information).
   */
  @Override
  public BlockMeta newBlockMeta(final long fileId, final LocatedBlock locatedBlock) throws IOException {
    BlockMeta blockMeta = new BlockMeta();
    blockMeta.setFileId(fileId);
    blockMeta.setOffSet(locatedBlock.getStartOffset());
    blockMeta.setLength(locatedBlock.getBlockSize());
    return blockMeta;
  }
}
