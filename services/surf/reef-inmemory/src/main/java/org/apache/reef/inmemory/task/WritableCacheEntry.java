package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.exceptions.BlockNotWritableException;
import org.apache.reef.inmemory.common.exceptions.BlockWritingException;

import java.io.IOException;

/**
 * Cache entry that writes data from clients.
 */
public class WritableCacheEntry implements CacheEntry {
  private final BlockWriter blockWriter;
  private boolean deletedManually = false;

  WritableCacheEntry(final BlockWriter blockWriter) {
    this.blockWriter = blockWriter;
  }

  @Override
  public byte[] getData(final int index) throws BlockWritingException {
    return blockWriter.getData(index);
  }

  @Override
  public long writeData(byte[] data, long offset, boolean isLastPacket) throws BlockNotWritableException, IOException {
    blockWriter.writeData(data, offset);
    if (isLastPacket) {
      blockWriter.completeWrite();
    }
    return blockWriter.getTotalWritten();
  }

  @Override
  public BlockId getBlockId() {
    return blockWriter.getBlockId();
  }

  @Override
  public boolean isPinned() {
    return blockWriter.isPinned();
  }

  @Override
  public long getBlockSize() {
    return blockWriter.getBlockSize();
  }

  @Override
  public void markAsDeleted() {
    deletedManually = true;
  }

  @Override
  public boolean isDeletedManually() {
    return deletedManually;
  }
}
