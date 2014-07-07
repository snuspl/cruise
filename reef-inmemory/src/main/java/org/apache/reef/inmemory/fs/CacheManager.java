package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.task.RunningTask;
import org.apache.reef.inmemory.cache.CacheStatusMessage;

import java.util.List;

/**
 * Supports FS-agnostic Task Management operations. Implementing classes must
 * support concurrent calls from multiple threads.
 */
public interface CacheManager {
  void requestEvaluator(int count, int memory);
  void requestEvaluator(int count);
  void submitContextAndTask(AllocatedEvaluator allocatedEvaluator);

  public boolean addRunningTask(RunningTask task);
  public void removeRunningTask(String taskId);

  /**
   * Return a view of running Caches. The returned list
   * is a copy -- it does not change as caches get updated.
   */
  public List<CacheNode> getCaches();

  CacheNode getCache(String taskId);

  public void handleUpdate(String taskId, CacheStatusMessage status);
}
