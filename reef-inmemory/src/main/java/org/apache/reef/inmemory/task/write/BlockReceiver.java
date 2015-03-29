package org.apache.reef.inmemory.task.write;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;

import java.io.IOException;

/**
 * Implementing this interface allows to write data into block.
 * This contains how many replica to have in the base FS
 * and Synchronization method (write-back / write-through)
 */
public interface BlockReceiver {

  /**
   * @return Block Identifier
   */
  public BlockId getBlockId();

  /**
   * @return Size of the block
   */
  public long getBlockSize();

  /**
   * @return Whether block is configured for pinning
   */
  public boolean isPinned();

  // TODO Delete this method, and move it to the CacheEntry.
  // TODO A similar method could be used to test.
  public byte[] getData(int index) throws BlockLoadingException;

  /**
   * Add data into block loader.
   * @param data data to add
   * @param offset offset of the data
   */
  public void writeData(byte[] data, long offset) throws IOException;

  /**
   * Called when the last packet of the block arrives.
   * Before complete, getData() for this block throws BlockLoadingException.
   */
  public void completeWrite();

  /**
   * Return the amount of data written.
   */
  public long getTotalWritten();
}
