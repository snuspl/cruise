package org.apache.reef.inmemory.task.write;

import com.sun.corba.se.spi.ior.Writeable;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.replication.SyncMethod;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.BlockLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides a way to load the content of block. The block is written by Surf
 * rather than loading from Underlying FS. Synchronization occurs with underFS
 * as specified by policy.
 * This class implements BlockLoader and Writable interface
 */
public class WritableBlockLoader implements BlockLoader, BlockReceiver {
  private final static Logger LOG = Logger.getLogger(Writeable.class.getName());

  private final BlockId blockId;
  private final int bufferSize;
  private final boolean pinned;
  private final long blockSize;
  private long totalWrite = 0;

  private Map<Integer, ByteBuffer> data;

  private final SyncMethod syncMethod;
  private final int baseReplicationFactor;
  private long expectedOffset = 0;

  public WritableBlockLoader(BlockId id, boolean pin, int bufferSize, int baseReplicationFactor, SyncMethod syncMethod){
    this.blockId = id;
    this.blockSize = id.getBlockSize();
    this.data = new HashMap<>();

    this.pinned = pin;
    this.bufferSize = bufferSize;
    this.baseReplicationFactor = baseReplicationFactor;
    this.syncMethod = syncMethod;
  }

  @Override
  public void loadBlock() {
    LOG.log(Level.INFO, "Enable Block {0} to write data", this.blockId);
  }

  @Override
  public BlockId getBlockId() {
    return this.blockId;
  }

  @Override
  public boolean isPinned() {
    return this.pinned;
  }

  @Override
  public byte[] getData(int index) throws BlockLoadingException {
    if (!data.containsKey(index)) {
      throw new BlockLoadingException(totalWrite);
    }
    assert this.data.get(index).hasArray();// TODO delete this;
    return this.data.get(index).array();
  }

  @Override
  public int getBufferSize() {
    return this.bufferSize;
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
      ByteBuffer buf = getBuffer(index);
      int toWrite = Math.min(bufferSize - innerOffset, data.length - nWritten);
      buf.put(data, nWritten, toWrite);

      index++;
      innerOffset = 0;
      nWritten += toWrite;
    }

    totalWrite += nWritten;
    updateValidOffset(offset, data.length);
  }

  @Override
  public int getBaseReplicationFactor() {
    return this.baseReplicationFactor;
  }

  @Override
  public SyncMethod getSyncMethod() {
    return this.syncMethod;
  }

  /**
   * Get the buffer to write data into. If the buffer for the range
   * does not exist, create one and put in the cache
   * @param index The index of buffer stored in the cache
   * @return The byte buffer
   */
  private synchronized ByteBuffer getBuffer(final int index) {
    if (!data.containsKey(index)) {
      // If blockSize is smaller than blockSize, then the blockSize will cover the whole data
      final ByteBuffer buf = ByteBuffer.allocate(Math.min(bufferSize, (int)blockSize));
      data.put(index, buf);
    }
    return data.get(index);
  }

  /**
   * Update the valid offsets.
   * @param received The offset of packet received.
   * @param packetLength The length of packet received.
   */
  private void updateValidOffset(final long received, final int packetLength) {
    /*
     * TODO If we send/receive packets with multi-thread, the packets could be out of order.
     * So we should maintain all the possible offset values as a set.
     */
    expectedOffset = received + packetLength;
  }

  /**
   * Determine offset of the packet is valid. It aims to avoid
   * packet duplicate or miss.
   * @param offset The offset of the packet
   * @return {@code true} if the offset is valid to receive
   */
  private boolean isValidOffset(final long offset) {
    return expectedOffset == offset;
  }


  public long getTotalWritten() {
    return this.totalWrite;
  }
}
