package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.cache.CacheStatusMessage;

import java.io.IOException;
import java.util.List;

/**
 * Supports FS-agnostic Task Management operations. Implementing classes must
 * support concurrent calls from multiple threads.
 */
public interface CacheManager<T> {
  public boolean addRunningTask(RunningTask task);
  public void removeRunningTask(String taskId);

  /**
   * Return a view of running Caches. The returned list
   * is a copy -- it does not change as caches get updated.
   */
  public List<CacheNode> getCaches();

  public void handleUpdate(String taskId, CacheStatusMessage status);
  public void clear(String taskId);
  public void clearAll();
  public void addBlock(String taskId, T msg);
}
