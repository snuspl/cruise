package org.apache.reef.inmemory.common.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.common.BlockIdFactory;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;

public final class HdfsBlockIdFactory implements BlockIdFactory<LocatedBlock, HdfsBlockId> {

  @Inject
  public HdfsBlockIdFactory() {
  }

  /**
   * Create a new HdfsBlockId using information from BlockInfo
   */
  @Override
  public HdfsBlockId newBlockId(final BlockInfo blockInfo) {
    return new HdfsBlockId(
            blockInfo.getFilePath(),
            blockInfo.getOffSet(),
            blockInfo.getBlockId(),
            blockInfo.getLength(),
            blockInfo.getGenerationStamp(),
            blockInfo.getNamespaceId(),
            blockInfo.getToken());
  }

  /**
   * Create a new HdfsBlockId using information from LocatedBlock
   */
  @Override
  public HdfsBlockId newBlockId(final String filePath, final LocatedBlock locatedBlock) throws IOException {
    return new HdfsBlockId(
            filePath,
            locatedBlock.getStartOffset(),
            locatedBlock.getBlock().getBlockId(),
            locatedBlock.getBlockSize(),
            locatedBlock.getBlock().getGenerationStamp(),
            locatedBlock.getBlock().getBlockPoolId(),
            locatedBlock.getBlockToken().encodeToUrlString());
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
   * Create a new BlockInfo using identifying information from LocatedBlock. Does /not/ copy
   * location information (as it is not identifying information).
   */
  @Override
  public BlockInfo newBlockInfo(final String filePath, final LocatedBlock locatedBlock) throws IOException {
    BlockInfo blockInfo = new BlockInfo();

    blockInfo.setFilePath(filePath);
    blockInfo.setOffSet(locatedBlock.getStartOffset());
    blockInfo.setBlockId(locatedBlock.getBlock().getBlockId());
    blockInfo.setLength(locatedBlock.getBlockSize());
    blockInfo.setNamespaceId(locatedBlock.getBlock().getBlockPoolId());
    blockInfo.setGenerationStamp(locatedBlock.getBlock().getGenerationStamp());
    blockInfo.setToken(locatedBlock.getBlockToken().encodeToUrlString());

    return blockInfo;
  }

  /**
   * Create a new BlockInfo with the assigned BlockId and allocated nodes.
   * Fields unknown at the creation time are marked as -1 or null.
   * TODO These fields will be removed once we loose the tight dependencies on HDFS
   */
  @Override
  public BlockInfo newBlockInfo(HdfsBlockId blockId, List<NodeInfo> nodes) {
    BlockInfo blockInfo = new BlockInfo();

    blockInfo.setFilePath(blockId.getFilePath());
    blockInfo.setOffSet(blockId.getOffset());
    blockInfo.setBlockId(-1);
    blockInfo.setLength(blockId.getBlockSize());
    blockInfo.setGenerationStamp(-1);
    blockInfo.setNamespaceId(null);
    blockInfo.setToken(null);
    blockInfo.setLocations(nodes);
    return blockInfo;
  }
}
