package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.exceptions.BlockNotWritableException;
import org.apache.reef.inmemory.common.exceptions.BlockWritingException;
import org.apache.reef.inmemory.task.write.BlockReceiver;

import java.io.IOException;

/**
 * Cache entry that writes data from clients.
 */
public class WritableCacheEntry implements CacheEntry {
  private final BlockReceiver blockReceiver;

  WritableCacheEntry(final BlockReceiver blockReceiver) {
    this.blockReceiver = blockReceiver;
  }

  @Override
  public byte[] getData(final int index) throws BlockWritingException {
    return this.blockReceiver.getData(index);
  }

  @Override
  public long writeData(byte[] data, long offset, boolean isLastPacket) throws BlockNotWritableException, IOException {
    blockReceiver.writeData(data, offset);
    if (isLastPacket) {
      blockReceiver.completeWrite();
    }
    return blockReceiver.getTotalWritten();
  }

  @Override
  public BlockId getBlockId() {
    return blockReceiver.getBlockId();
  }

  @Override
  public boolean isPinned() {
    return blockReceiver.isPinned();
  }

  @Override
  public long getBlockSize() {
    return blockReceiver.getBlockSize();
  }
}
