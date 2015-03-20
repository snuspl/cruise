package org.apache.reef.inmemory.driver;

/**
 * Interface for sending messages to Cache nodes.
 * A new implementation should be create for each Base FS, because
 * the block information <T> is FS-dependant.
 */
public interface CacheNodeMessenger<T> {
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
}
