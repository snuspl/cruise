package org.apache.reef.inmemory.cache;

/**
 * Implementation of Block identification for HDFS, based on blockId.
 * The implementation may have to change in the future, based on
 * experience with HDFS.
 */

public final class HdfsBlockId extends BlockId{

  private final long blockId;
  private final String fs = "hdfs";

  public HdfsBlockId(final long blockId) {
    this.blockId = blockId;
  }

  @Override
  public String getFs() {
    return fs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HdfsBlockId that = (HdfsBlockId) o;

    if (blockId != that.blockId) return false;
    if (!fs.equals(that.fs)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (blockId ^ (blockId >>> 32));
    result = 31 * result + fs.hashCode();
    return result;
  }
}
