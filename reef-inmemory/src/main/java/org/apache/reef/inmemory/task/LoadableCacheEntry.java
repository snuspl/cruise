package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotWritableException;

/**
 * Cache entry that loads data from BaseFS.
 */
public class LoadableCacheEntry implements CacheEntry {
  private final BlockLoader blockLoader;

  LoadableCacheEntry(final BlockLoader blockLoader) {
    this.blockLoader = blockLoader;
  }

  @Override
  public byte[] getData(final int index) throws BlockLoadingException {
    return this.blockLoader.getData(index);
  }

  @Override
  public long writeData(byte[] data, long offset, boolean isLastPacket) throws BlockNotWritableException {
    throw new BlockNotWritableException();
  }

  @Override
  public BlockId getBlockId() {
    return blockLoader.getBlockId();
  }

  @Override
  public boolean isPinned() {
    return blockLoader.isPinned();
  }

  @Override
  public long getBlockSize() {
    return blockLoader.getBlockSize();
  }
}
