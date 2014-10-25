package org.apache.reef.inmemory.driver;

import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.task.BlockId;

/**
 * Interface for sending messages to Cache nodes.
 * A new implementation should be create for each Base FS, because
 * the block information <T> is FS-dependant.
 */
public interface CacheMessenger<T> {
  /**
   * Remove blocks stored at the Cache
   */
  public void clear(String taskId);

  /**
   * Remove blocks stored at all Caches
   */
  public void clearAll();

  /**
   * Send a block load request to the Cache
   */
  public void addBlock(String taskId, T msg);

  /**
   * Send a block allocate request to the Cache
   */
  public void allocateBlock(String taskId, BlockId blockId, Action action);
}
