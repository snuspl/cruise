package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.task.BlockId;

/**
 * Implementation of BlockId. Currently path, offset, blockSize are used,
 * but can contain more (or less) information if needed.
 */
public class BlockIdImpl implements BlockId {
  private final String filePath;
  private final long offset;
  private final long blockSize;

  public BlockIdImpl(final String filePath, final long offset, final long blockSize) {
    this.filePath = filePath;
    this.offset = offset;
    this.blockSize = blockSize;
  }

  @Override
  public String getFilePath() {
    return filePath;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public long getBlockSize() {
    return blockSize;
  }
}
