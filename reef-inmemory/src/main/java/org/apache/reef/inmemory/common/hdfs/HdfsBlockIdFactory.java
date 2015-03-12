package org.apache.reef.inmemory.common.hdfs;

import org.apache.reef.inmemory.common.BlockIdFactory;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;

import javax.inject.Inject;
import java.util.List;

public final class HdfsBlockIdFactory implements BlockIdFactory<HdfsBlockId> {

  @Inject
  public HdfsBlockIdFactory() {
  }

  /**
   * Create a new HdfsBlockId using information from BlockMeta
   */
  @Override
  public HdfsBlockId newBlockId(final BlockMeta blockMeta) {
    return new HdfsBlockId(
            blockMeta.getFilePath(),
            blockMeta.getOffSet(),
            blockMeta.getBlockId(),
            blockMeta.getLength(),
            blockMeta.getGenerationStamp(),
            blockMeta.getNamespaceId(),
            blockMeta.getToken());
  }

  /**
   * Assign a BlockId to create a new block when writing data directly to Surf.
   * Fields unknown at the creation time are marked as -1 or null.
   * TODO These fields will be removed once we loose the tight dependencies on HDFS
   */
  @Override
  public HdfsBlockId newBlockId(String filePath, long offset, long blockSize) {
    return new HdfsBlockId(
      filePath,
      offset,
      -1, // BlockId
      blockSize,
      -1, // GenerationTimestamp
      null, // PoolId
      null // EncodedToken
    );
  }

  /**
   * Create a new BlockMeta with the assigned BlockId and allocated nodes.
   * Fields unknown at the creation time are marked as -1 or null.
   * TODO These fields will be removed once we loose the tight dependencies on HDFS
   */
  @Override
  public BlockMeta newBlockMeta(HdfsBlockId blockId, List<NodeInfo> nodes) {
    BlockMeta blockMeta = new BlockMeta();

    blockMeta.setFilePath(blockId.getFilePath());
    blockMeta.setOffSet(blockId.getOffset());
    blockMeta.setBlockId(-1);
    blockMeta.setLength(blockId.getBlockSize());
    blockMeta.setGenerationStamp(-1);
    blockMeta.setNamespaceId(null);
    blockMeta.setToken(null);
    blockMeta.setLocations(nodes);
    return blockMeta;
  }
}
