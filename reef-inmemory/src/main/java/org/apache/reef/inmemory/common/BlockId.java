package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.common.entity.BlockMeta;

import java.io.Serializable;

/**
 * Block ID to identify blocks cached at each Task.
 */
public final class BlockId implements Serializable {
  private final long fileId;
  private final long offset;

  /**
   * Create a block id with information to identify this block.
   * @param fileId The file which owns this block.
   * @param offset The offset from the start of the file.
   */
  public BlockId(final long fileId, final long offset) {
    this.fileId = fileId;
    this.offset = offset;
  }

  /**
   * Create a block id from BlockMeta object.
   * @param blockMeta Metadata of this block.
   */
  public BlockId(final BlockMeta blockMeta) {
    this(blockMeta.getFileId(), blockMeta.getOffSet());
  }

  /**
   * Return the id of file.
   */
  public long getFileId() {
    return fileId;
  }

  /**
   * Return the offset of this block from the start of the file.
   */
  public long getOffset() {
    return offset;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BlockId blockId = (BlockId) o;
    return offset == blockId.offset && fileId == blockId.fileId;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = (31 * result) + (int) (fileId ^ (fileId >>> 32));
    result = (31 * result) + (int) (offset ^ (offset >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "BlockId{" +
            "fileId='" + fileId + '\'' +
            ", offset=" + offset +
            '}';
  }
}
