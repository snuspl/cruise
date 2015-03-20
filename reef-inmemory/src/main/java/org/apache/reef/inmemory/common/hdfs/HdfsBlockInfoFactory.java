package org.apache.reef.inmemory.common.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.common.BaseFsBlockInfoFactory;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockInfo;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Implementation of BaseFsBlockInfoFactory for HDFS.
 */
public class HdfsBlockInfoFactory implements BaseFsBlockInfoFactory<LocatedBlock, HdfsBlockInfo> {
  @Inject
  public HdfsBlockInfoFactory() {
  }

  /**
   * Create a new HdfsBlockInfo using information from LocatedBlock
   */
  @Override
  public HdfsBlockInfo newBlockInfo(final String filePath, final LocatedBlock locatedBlock) throws IOException {
    return new HdfsBlockInfo(
            filePath,
            locatedBlock.getStartOffset(),
            locatedBlock.getBlock().getBlockId(),
            locatedBlock.getBlockSize(),
            locatedBlock.getBlock().getGenerationStamp(),
            locatedBlock.getBlock().getBlockPoolId(),
            locatedBlock.getBlockToken().encodeToUrlString());
  }
}
