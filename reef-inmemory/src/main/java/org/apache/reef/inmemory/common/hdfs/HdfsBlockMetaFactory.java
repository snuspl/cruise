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
   * Create a new BlockMeta using identifying information from LocatedBlock. Does /not/ copy
   * location information (as it is not identifying information).
   */
  @Override
  public BlockMeta newBlockMeta(final long fileId, final LocatedBlock locatedBlock) {
    BlockMeta blockMeta = new BlockMeta();
    blockMeta.setFileId(fileId);
    blockMeta.setOffSet(locatedBlock.getStartOffset());
    blockMeta.setLength(locatedBlock.getBlockSize());
    return blockMeta;
  }
}
