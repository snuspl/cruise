package org.apache.reef.inmemory.task.write;

import org.apache.reef.inmemory.task.BlockId;

/**
 * Block Identifier used to distinguish blocks written by Surf.
 */
public class WritableBlockId implements BlockId {
  private String path;
  private long offset;
  private long blockSize;

  /**
   * We can distinguish each single block with [path, offset] tuple,
   * and the blockSize is needed to figure out how many bytes can be
   * loaded/written in the block.
   * @param path Path of the file which contains this block.
   * @param offset Offset of this block inside the file.
   * @param blockSize Size of this block.
   */
  public WritableBlockId(final String path, final long offset, final long blockSize) {
    this.path = path;
    this.offset = offset;
    this.blockSize = blockSize;
  }

  @Override
  public String getFilePath() {
    return this.path;
  }

  @Override
  public long getOffset() {
    return this.offset;
  }

  @Override
  public long getBlockSize() {
    return this.blockSize;
  }
}
