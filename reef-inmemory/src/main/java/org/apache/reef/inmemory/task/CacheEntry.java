package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotWritableException;
import org.apache.reef.inmemory.common.exceptions.BlockWritingException;

import java.io.IOException;

/**
 * An entry of cache that holds the data.
 */
public interface CacheEntry {
  /**
   * Get the data that this entry holds, which is split in chunks.
   * @param index Chunk index inside block.
   * @return The byte array containing the data of the chunk.
   * @throws BlockLoadingException
   * @throws BlockWritingException
   */
  public byte[] getData(final int index) throws BlockLoadingException, BlockWritingException;

  /**
   * Write data packet into this entry.
   * @param data The data packet to write.
   * @param offset The offset from the start of Block.
   * @param isLastPacket {@code true} if this packet is the last piece to write.
   * @return The amount of data (in bytes) written in this entry.
   * @throws BlockNotWritableException If this block is not allowed to write data.
   * @throws IOException
   */
  public long writeData(byte[] data, long offset, boolean isLastPacket) throws BlockNotWritableException, IOException;

  /**
   * @return BlockId assigned to this CacheEntry.
   */
  public BlockId getBlockId();

  /**
   * @return {@code true} if this block is pinned.
   */
  public boolean isPinned();

  /**
   * @return the size of this block.
   */
  public long getBlockSize();

  /**
   * Mark this entry is deleted by user's request, not by eviction.
   */
  public void markAsDeleted();

  /**
   * @return {@code true} if this block is deleted manually.
   */
  public boolean isDeletedManually();
}
