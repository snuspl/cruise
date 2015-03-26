package org.apache.reef.inmemory.task.write;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.task.BlockLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides a way to load the content of block. The block is written by Surf
 * rather than loading from base FS. Synchronization occurs with base FS
 * as specified by policy.
 * This class implements BlockLoader and BlockReceiver interface
 */
public class WritableBlockLoader implements BlockLoader, BlockReceiver {
  private final static Logger LOG = Logger.getLogger(WritableBlockLoader.class.getName());

  private final BlockId blockId;
  private final int bufferSize;
  private final boolean pinned;
  private final long blockSize;
  private long totalWritten = 0;
  private boolean isComplete = false;

  private List<ByteBuffer> data;

  private long expectedOffset = 0;

  public WritableBlockLoader(final BlockId id, final long blockSize, final boolean pin, final int bufferSize) {
    this.blockId = id;
    this.blockSize = blockSize;
    this.data = new ArrayList<>();

    this.pinned = pin;
    this.bufferSize = bufferSize;
  }

  @Override
  public void loadBlock() {
    LOG.log(Level.SEVERE, "loadBlock() should not be called for WritableBlockLoader. BlockId : {0}", blockId.toString());
  }

  @Override
  public BlockId getBlockId() {
    return this.blockId;
  }

  @Override
  public long getBlockSize() {
    return blockSize;
  }

  @Override
  public boolean isPinned() {
    return this.pinned;
  }

  @Override
  public byte[] getData(int index) throws BlockLoadingException {
    // If the date is not completely written for this block, throw BlockLoadingException.
    if (!isComplete || index >= data.size()) {
      throw new BlockLoadingException(totalWritten);
    }

    final ByteBuffer buf = this.data.get(index);
    if(buf.position() != bufferSize) {
      final byte[] bArray = new byte[buf.position()];
      buf.position(0);
      buf.get(bArray);
      return bArray;
    } else {
      return buf.array();
    }
  }

  @Override
  public void writeData(final byte[] data, final long offset) throws IOException {
    if (offset + data.length > blockSize) {
      throw new IOException("The data exceeds the capacity of block. Offset : " + offset
        + " , Packet length : " + data.length + " Block size : " + blockSize);
    } else if (!isValidOffset(offset)) {
      throw new IOException("Received packet with an invalid offset " + offset);
    }

    int index = (int) (offset / bufferSize);
    int innerOffset = (int) (offset % bufferSize);
    int nWritten = 0;

    while (nWritten < data.length) {
      final ByteBuffer buf = getBuffer(index);
      final int toWrite = Math.min(bufferSize - innerOffset, data.length - nWritten);
      buf.put(data, nWritten, toWrite);

      index++;
      innerOffset = 0;
      nWritten += toWrite;
    }

    totalWritten += nWritten;
    updateValidOffset(offset, data.length);
  }

  /**
   * Called when the last packet of the block arrives.
   * Before complete, getData() for this block throws BlockLoadingException.
   */
  public void completeWrite() {
    this.isComplete = true;
  }

  /**
   * Return the amount of data written.
   */
  public long getTotalWritten() {
    return this.totalWritten;
  }

  /**
   * Get the buffer to write data into. If the buffer for the range
   * does not exist, create one and put in the cache on demand.
   * @param index The index of buffer stored in the cache
   * @return The byte buffer
   */
  private synchronized ByteBuffer getBuffer(final int index) {
    if (index >= data.size()) {
      // If blockSize is smaller than blockSize, then the blockSize will cover the whole data
      final ByteBuffer buf = ByteBuffer.allocate(Math.min(bufferSize, (int)blockSize));
      data.add(buf);
    }
    return data.get(index);
  }

  /**
   * Update the valid offsets.
   * @param previousOffset The valid offset for the previous packet.
   * @param packetLength The length of packet received.
   */
  private void updateValidOffset(final long previousOffset, final int packetLength) {
    expectedOffset = previousOffset + packetLength;
  }

  /**
   * Determine offset of the packet is valid in order to avoid duplicate or miss.
   * The assumption is the packets always come in-order.
   * @param offset The offset of the packet
   * @return {@code true} if the offset is valid to receive
   */
  private boolean isValidOffset(final long offset) {
    return expectedOffset == offset;
  }
}
