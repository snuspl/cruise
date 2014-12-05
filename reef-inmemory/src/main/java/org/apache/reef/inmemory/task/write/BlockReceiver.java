package org.apache.reef.inmemory.task.write;

import java.io.IOException;

/**
 * Implementing this interface allows to write data into block.
 * This contains how many replica to have in the base FS
 * and Synchronization method (write-back / write-through)
 */
public interface BlockReceiver {

  /**
   * Add data into block loader.
   * @param data data to add
   * @param offset offset of the data
   */
  public void writeData(byte[] data, long offset) throws IOException;
}
