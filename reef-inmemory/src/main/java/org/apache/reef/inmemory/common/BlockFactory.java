package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.task.BlockId;

/**
 * Factory that creates Block objects (using information from other Block objects)
 */
public interface BlockFactory {

  /**
   * Create a new BlockId using information from BlockInfo
   */
  public BlockId newBlockId(BlockInfo blockInfo);
}
