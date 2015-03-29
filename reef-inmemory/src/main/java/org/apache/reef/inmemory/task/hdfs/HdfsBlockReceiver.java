package org.apache.reef.inmemory.task.hdfs;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.exceptions.BlockWritingException;
import org.apache.reef.inmemory.task.BlockReceiver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * BlockReceiver implementation for HDFS.
 */
public class HdfsBlockReceiver implements BlockReceiver {

  private final BlockId blockId;
  private final int bufferSize;
  private final boolean pinned;
  private final long blockSize;
  private long totalWritten = 0;
  private long expectedOffset = 0;
  private boolean isComplete = false;

  private List<ByteBuffer> data;

  public HdfsBlockReceiver(final BlockId id, final long blockSize, final boolean pin, final int bufferSize) {
    this.blockId = id;
    this.blockSize = blockSize;
    this.data = new ArrayList<>();

    this.pinned = pin;
    this.bufferSize = bufferSize;
  }

  @Override
  public BlockId getBlockId() {
    return this.blockId;
  }

  @Override
  public long getBlockSize() {
    return this.blockSize;
  }

  @Override
  public boolean isPinned() {
    return pinned;
  }

  @Override
  public byte[] getData(int index) throws BlockWritingException {
    // If the date is not completely written for this block, throw BlockLoadingException.
    if (!isComplete || index >= data.size()) {
      throw new BlockWritingException(totalWritten);
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
  public void writeData(byte[] data, long offset) throws IOException {
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

  @Override
  public void completeWrite() {
    this.isComplete = true;
  }

  @Override
  public long getTotalWritten() {
    return totalWritten;
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
