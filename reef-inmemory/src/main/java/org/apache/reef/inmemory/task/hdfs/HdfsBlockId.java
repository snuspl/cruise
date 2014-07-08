package org.apache.reef.inmemory.task.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.common.entity.BlockInfo;

import java.io.IOException;
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
public final class HdfsBlockId implements BlockId, Serializable {

  private final long blockId;
  private final long blockSize;
  private final long generationTimestamp;
  private final String poolId;
  private final String encodedToken;

  public HdfsBlockId(final long blockId,
                     final long blockSize,
                     final long generationTimestamp,
                     final String poolId,
                     final String encodedToken) {
    this.blockId = blockId;
    this.blockSize = blockSize;
    this.generationTimestamp = generationTimestamp;
    this.poolId = poolId;
    this.encodedToken = encodedToken;
  }

  public static HdfsBlockId copyBlock(LocatedBlock locatedBlock) throws IOException {
    return new HdfsBlockId(
            locatedBlock.getBlock().getBlockId(),
            locatedBlock.getBlockSize(),
            locatedBlock.getBlock().getGenerationStamp(),
            locatedBlock.getBlock().getBlockPoolId(),
            locatedBlock.getBlockToken().encodeToUrlString());
  }

  public static HdfsBlockId copyBlock(BlockInfo blockInfo) {
    return new HdfsBlockId(
            blockInfo.getBlockId(),
            blockInfo.getLength(),
            blockInfo.getGenerationStamp(),
            blockInfo.getNamespaceId(),
            blockInfo.getToken());
  }

  public long getBlockId() {
    return blockId;
  }

  public long getBlockSize() {
    return blockSize;
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

    HdfsBlockId that = (HdfsBlockId) o;

    if (blockId != that.blockId) return false;
    if (blockSize != that.blockSize) return false;
    if (generationTimestamp != that.generationTimestamp) return false;
    if (encodedToken != null ? !encodedToken.equals(that.encodedToken) : that.encodedToken != null) return false;
    if (poolId != null ? !poolId.equals(that.poolId) : that.poolId != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (blockId ^ (blockId >>> 32));
    result = 31 * result + (int) (blockSize ^ (blockSize >>> 32));
    result = 31 * result + (int) (generationTimestamp ^ (generationTimestamp >>> 32));
    result = 31 * result + (poolId != null ? poolId.hashCode() : 0);
    result = 31 * result + (encodedToken != null ? encodedToken.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return blockId + ", "
            + blockSize + ", "
            + generationTimestamp + ", "
            + poolId + ", "
            + encodedToken;
  }
}
