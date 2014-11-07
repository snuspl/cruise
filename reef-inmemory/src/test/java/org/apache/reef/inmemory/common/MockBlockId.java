package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.task.BlockId;

/**
 * A simple BlockId implementation for testing
 */
public final class MockBlockId implements BlockId {

  private final String filePath;
  private final long offset;
  private final long blockSize;

  public MockBlockId(final String filePath,
                     final long offset,
                     final long blockSize) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MockBlockId that = (MockBlockId) o;

    if (filePath != null ? !filePath.equals(that.filePath) : that.filePath != null) return false;
    if (offset != that.offset) return false;
    if (blockSize != that.blockSize) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (filePath != null ? filePath.hashCode() : 0);
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    result = 31 * result + (int) (blockSize ^ (blockSize >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "MockBlockId{" +
            "filePath='" + filePath +
            "', offset='" + offset +
            "', blockSize='" + blockSize + '\'' +
            '}';
  }
}
