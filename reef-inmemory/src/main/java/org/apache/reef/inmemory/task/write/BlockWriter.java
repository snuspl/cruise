package org.apache.reef.inmemory.task.write;

import org.apache.reef.inmemory.common.replication.SyncMethod;

import java.io.IOException;

/**
 * Implementing this interface allows to write data into block.
 * This contains how many replica to have in the UnderFS
 * and Synchronization method (write-back / write-through)
 */
public interface BlockWriter {

  /**
   * Add data into block loader.
   * @param data data to add
   * @param offset offset of the data
   */
  public void writeData(byte[] data, long offset) throws IOException;

  /**
   * @return the number of blocks to be replicated in the baseFS
   */
  public int getBaseReplicationFactor();

  /**
   * SyncMethod is either write-back or write-through
   * @return How synchronize the data between the cache and the baseFS
   */
  public SyncMethod getSyncMethod();
}
