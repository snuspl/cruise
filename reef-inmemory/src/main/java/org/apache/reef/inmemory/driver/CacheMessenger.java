package org.apache.reef.inmemory.driver;

/**
 * Interface for sending messages to Cache nodes.
 * A new implementation should be create for each Base FS, because
 * the block information <T> is FS-dependant.
 */
public interface CacheMessenger<T> {
  public void clear(String taskId);
  public void clearAll();
  public void addBlock(String taskId, T msg);
}
