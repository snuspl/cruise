package org.apache.reef.inmemory.cache;

public class HdfsBlockId extends BlockId{

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
