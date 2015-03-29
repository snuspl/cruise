package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.task.write.BlockReceiver;

import java.util.List;

/**
 * An entry of cache that holds the data.
 */
public final class CacheEntry {
  private final BlockId blockId;
  private boolean pinned;
  private long blockSize;

  private final BlockLoader blockLoader;
  private final BlockReceiver blockReceiver;
  // TODO The data will be loaded/written here.
  private List<byte[]> data = null;


  // TODO Create BlockLoader inside here? or use a factory method.
  public CacheEntry(final BlockLoader blockLoader) {
    this.blockId = blockLoader.getBlockId();
    this.blockLoader = blockLoader;
    this.blockReceiver = null;
    this.pinned = blockLoader.isPinned();
    this.blockSize = blockLoader.getBlockSize();
  }

  public CacheEntry(final BlockReceiver blockReceiver) {
    this.blockId = blockReceiver.getBlockId();
    this.blockLoader = null;
    this.blockReceiver = blockReceiver;
    this.pinned = blockReceiver.isPinned();
    this.blockSize = blockReceiver.getBlockSize();
  }

  public byte[] getData(final int index) throws BlockLoadingException {
    // TODO return the data of this.
    if (blockLoader != null) {
      return this.blockLoader.getData(index);
    } else {
      return this.blockReceiver.getData(index);
    }
  }

  public BlockId getBlockId() {
    return blockId;
  }

  public boolean isPinned() {
    return pinned;
  }

  public long getBlockSize() {
    return blockSize;
  }

  public BlockReceiver getBlockReceiver() {
    return blockReceiver;
  }
}
