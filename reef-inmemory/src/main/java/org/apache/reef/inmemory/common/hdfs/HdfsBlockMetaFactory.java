package org.apache.reef.inmemory.common.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.common.BaseFsBlockMetaFactory;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Implementation of BlockMetaFactory for HDFS.
 */
public class HdfsBlockMetaFactory implements BaseFsBlockMetaFactory<LocatedBlock, HdfsBlockId> {
  @Inject
  public HdfsBlockMetaFactory() {
  }

  @Override
  public HdfsBlockId newBlockId(BlockMeta blockMeta) {
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
   * Create a new BlockMeta using identifying information from LocatedBlock. Does /not/ copy
   * location information (as it is not identifying information).
   */
  @Override
  public BlockMeta newBlockMeta(final String filePath, final LocatedBlock locatedBlock) throws IOException {
    BlockMeta blockMeta = new BlockMeta();

    blockMeta.setFilePath(filePath);
    blockMeta.setOffSet(locatedBlock.getStartOffset());
    blockMeta.setBlockId(locatedBlock.getBlock().getBlockId());
    blockMeta.setLength(locatedBlock.getBlockSize());
    blockMeta.setNamespaceId(locatedBlock.getBlock().getBlockPoolId());
    blockMeta.setGenerationStamp(locatedBlock.getBlock().getGenerationStamp());
    blockMeta.setToken(locatedBlock.getBlockToken().encodeToUrlString());

    return blockMeta;
  }
}
