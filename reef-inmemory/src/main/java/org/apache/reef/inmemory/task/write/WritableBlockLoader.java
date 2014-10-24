package org.apache.reef.inmemory.task.write;

import com.sun.corba.se.spi.ior.Writeable;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.replication.SyncMethod;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.BlockLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
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
public class WritableBlockLoader implements BlockLoader, BlockWriter {
  private final static Logger LOG = Logger.getLogger(Writeable.class.getName());

  // TODO Constructor
  // TODO Introduce state?
  private final BlockId blockId;
  private final int bufferSize;
  private final boolean pinned;
  private final long blockSize;
  private long totalWrite = 0;

  private Map<Integer, byte[]> data;

  private final SyncMethod syncMethod;
  private final int baseReplicationFactor;

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
    LOG.log(Level.SEVERE, "This method is not supposed to call");
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
      // TODO
      throw new BlockLoadingException();
    }
    return this.data.get(index);
  }

  @Override
  public int getBufferSize() {
    return this.bufferSize;
  }

  @Override
  public void writeData(final byte[] data, final long offset) throws IOException {
    if (offset + data.length > blockSize)
      throw new IOException("The data exceeds the capacity of block");

    int index = (int) (offset / bufferSize);
    int innerOffset = (int) (offset % bufferSize);
    int nTotal = 0;

    // TODO Keep track of total amount of data
    while (nTotal < data.length) {
      ByteBuffer buf = getBuffer(index);
      buf.position(innerOffset);
      int toWrite = Math.min(bufferSize - innerOffset, data.length - nTotal);
      buf.put(data, nTotal, toWrite);

      index++;
      innerOffset = 0;
      nTotal += toWrite;
    }

    totalWrite += nTotal;
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
      final byte[] buffer = new byte[bufferSize];
      data.put(index, buffer);
    }
    return ByteBuffer.wrap(data.get(index));
  }

}
