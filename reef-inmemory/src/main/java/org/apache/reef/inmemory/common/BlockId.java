package org.apache.reef.inmemory.common;

import java.io.Serializable;

/**
 * Block ID to be used to identify blocks cached at each Task.
 * Implementing classes will be used as Map keys. Therefore, they must provide
 * well-formed equals() and hashCode() methods.
 */
public class BlockId implements Serializable {
  private final String filePath;
  private final long offset;
  private final long blockSize;

  public BlockId(final String filePath, final long offset, final long blockSize) {
    this.filePath = filePath;
    this.offset = offset;
    this.blockSize = blockSize;
  }

  public String getFilePath() {
    return filePath;
  }

  public long getOffset() {
    return offset;
  }

  public long getBlockSize() {
    return blockSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BlockId blockId = (BlockId) o;

    if (blockSize != blockId.blockSize) return false;
    if (offset != blockId.offset) return false;
    if (filePath != null ? !filePath.equals(blockId.filePath) : blockId.filePath != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = filePath != null ? filePath.hashCode() : 0;
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    result = 31 * result + (int) (blockSize ^ (blockSize >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "BlockIdImpl{" +
            "filePath='" + filePath + '\'' +
            ", offset=" + offset +
            ", blockSize=" + blockSize +
            '}';
  }
}
