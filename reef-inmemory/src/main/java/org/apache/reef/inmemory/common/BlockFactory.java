package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.task.BlockId;

public interface BlockFactory {
  public BlockId newBlockId(BlockInfo blockInfo);
}
