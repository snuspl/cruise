package org.apache.reef.inmemory.common.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.BlockFactory;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;

import javax.inject.Inject;
import java.io.IOException;

public final class HdfsBlockFactory implements BlockFactory {

  @Inject
  public HdfsBlockFactory() {
  }

  @Override
  public BlockId newBlockId(BlockInfo blockInfo) {
    return new HdfsBlockId(
            blockInfo.getBlockId(),
            blockInfo.getLength(),
            blockInfo.getGenerationStamp(),
            blockInfo.getNamespaceId(),
            blockInfo.getToken());
  }

  public HdfsBlockId newBlockId(LocatedBlock locatedBlock) throws IOException {
    return new HdfsBlockId(
            locatedBlock.getBlock().getBlockId(),
            locatedBlock.getBlockSize(),
            locatedBlock.getBlock().getGenerationStamp(),
            locatedBlock.getBlock().getBlockPoolId(),
            locatedBlock.getBlockToken().encodeToUrlString());
  }

  /**
   * Copies block identifying information from LocatedBlock into BlockInfo. Does /not/ copy
   * location information (as it is not identifying information).
   */
  public BlockInfo newBlockInfo(LocatedBlock locatedBlock) throws IOException {
    BlockInfo blockInfo = new BlockInfo();

    blockInfo.setBlockId(locatedBlock.getBlock().getBlockId());
    blockInfo.setOffSet(locatedBlock.getStartOffset());
    blockInfo.setLength(locatedBlock.getBlockSize());
    blockInfo.setNamespaceId(locatedBlock.getBlock().getBlockPoolId());
    blockInfo.setGenerationStamp(locatedBlock.getBlock().getGenerationStamp());
    blockInfo.setToken(locatedBlock.getBlockToken().encodeToUrlString());

    return blockInfo;
  }
}
