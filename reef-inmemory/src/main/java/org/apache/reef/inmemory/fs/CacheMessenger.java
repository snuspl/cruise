package org.apache.reef.inmemory.fs;

public interface CacheMessenger<T> {
  public void clear(String taskId);
  public void clearAll();
  public void addBlock(String taskId, T msg);
}
