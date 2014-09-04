package org.apache.reef.inmemory.task;

final class MockBlockId implements BlockId {

  private final long blockId;
  private final long blockSize;

  public MockBlockId(final long blockId,
                     final long blockSize) {
    this.blockId = blockId;
    this.blockSize = blockSize;
  }

  @Override
  public String getFilePath() {
    return "/mock/"+blockId;
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

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (blockId ^ (blockId >>> 32));
    result = 31 * result + (int) (blockSize ^ (blockSize >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "MockBlockId{" +
            "blockId='" + blockId + '\'' +
            '}';
  }
}
