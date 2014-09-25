package org.apache.reef.inmemory.task;

/**
 * A simple BlockId implementation for testing
 */
public final class MockBlockId implements BlockId {

  private final long blockId;
  private final long blockSize;
  private final String filePath;

  public MockBlockId(final long blockId,
                     final long blockSize,
                     final String filePath) {
    this.blockId = blockId;
    this.blockSize = blockSize;
    this.filePath = filePath;
  }

  @Override
  public String getFilePath() {
    return filePath;
  }

  @Override
  public long getOffset() {
    return 0;
  }

  @Override
  public long getUniqueId() {
    return blockId;
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

    if (blockId != that.blockId) return false;
    if (blockSize != that.blockSize) return false;
    if (filePath != null ? !filePath.equals(that.filePath) : that.filePath != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (blockId ^ (blockId >>> 32));
    result = 31 * result + (int) (blockSize ^ (blockSize >>> 32));
    result = 31 * result + (filePath != null ? filePath.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "MockBlockId{" +
            "blockId=" + blockId +
            ", blockSize=" + blockSize +
            ", filePath='" + filePath + '\'' +
            '}';
  }
}
