package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.common.entity.BlockMeta;

import java.io.Serializable;

/**
 * Block ID to identify blocks cached at each Task.
 */
public final class BlockId implements Serializable {
  private final String filePath;
  private final long offset;
  private final long blockSize;

  /**
   * Create a block id with information to identify this block.
   * @param filePath The file which consists of this block.
   * @param offset The offset from the start of the file.
   * @param blockSize The size of block.
   */
  public BlockId(final String filePath, final long offset, final long blockSize) {
    this.filePath = filePath; // TODO Replace filePath with another unique field (e.g. fileId)
    this.offset = offset;
    this.blockSize = blockSize;
  }

  /**
   * Create a block id from BlockMeta object.
   * @param blockMeta Metadata of this block.
   */
  public BlockId(final BlockMeta blockMeta) {
    this(blockMeta.getFilePath(), blockMeta.getOffSet(), blockMeta.getLength());
  }

  /**
   * Return the path of file.
   * TODO Replace filePath with another unique field (e.g. fileId)
   */
  public String getFilePath() {
    return filePath;
  }

  /**
   * Return the offset of this block from the start of the file.
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Return the size of block.
   */
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
    return "BlockId{" +
            "filePath='" + filePath + '\'' +
            ", offset=" + offset +
            ", blockSize=" + blockSize +
            '}';
  }
}
