package org.apache.reef.inmemory.task.hdfs;

import java.io.Serializable;

/**
 * Implementation of Block identification for HDFS, based on blockId.
 * The implementation may have to change in the future, based on
 * experience with HDFS.
 *
 * Block information is copied to this class because of deficiencies
 * in the other block classes:
 * - A Thrift-generated class cannot be used, as it returns a
 *   hashCode() of 0.
 * - HDFS's LocatedBlock cannot be used, as it does not define an equals()
 *   method.
 */
public final class HdfsBlockInfo implements Serializable {

  private final String filePath;
  private final long offset;
  private final long blockId;
  private final long blockSize;
  private final long generationTimestamp;
  private final String poolId;
  private final String encodedToken;

  public HdfsBlockInfo(final String filePath,
                       final long offset,
                       final long blockId,
                       final long blockSize,
                       final long generationTimestamp,
                       final String poolId,
                       final String encodedToken) {
    this.filePath = filePath;
    this.offset = offset;
    this.blockId = blockId;
    this.blockSize = blockSize;
    this.generationTimestamp = generationTimestamp;
    this.poolId = poolId;
    this.encodedToken = encodedToken;
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

  public long getUniqueId() {
    return blockId;
  }

  public long getGenerationTimestamp() {
    return generationTimestamp;
  }

  public String getPoolId() {
    return poolId;
  }

  public String getEncodedToken() {
    return encodedToken;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HdfsBlockInfo that = (HdfsBlockInfo) o;

    if (!filePath.equals(that.filePath)) return false;
    if (blockId != that.blockId) return false;
    if (blockSize != that.blockSize) return false;
    if (generationTimestamp != that.generationTimestamp) return false;
    if (!poolId.equals(that.poolId)) return false;
    if (!encodedToken.equals(that.encodedToken)) return false;
    if (offset != that.offset) return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = filePath != null ? filePath.hashCode() : 0;
    result = 31 * result + (int) (blockId ^ (blockId >>> 32));
    result = 31 * result + (int) (blockSize ^ (blockSize >>> 32));
    result = 31 * result + (int) (generationTimestamp ^ (generationTimestamp >>> 32));
    result = 31 * result + (poolId != null ? poolId.hashCode() : 0);
    result = 31 * result + (encodedToken != null ? encodedToken.hashCode() : 0);
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return filePath + ", "
            + blockId + ", "
            + blockSize + ", "
            + offset;
  }
}
