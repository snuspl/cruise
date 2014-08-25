package org.apache.reef.inmemory.driver;

import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.task.RunningTask;
import org.apache.reef.inmemory.common.CacheStatusMessage;

import java.util.List;

/**
 * Supports FS-agnostic Task Management operations. Implementing classes must
 * support concurrent calls from multiple threads.
 */
public interface CacheManager {

  /**
   * Request the specified number of evaluators with the memory size
   */
  void requestEvaluator(int count, int memory);

  /**
   * Request the specified number of evaluators with the default memory size
   */
  void requestEvaluator(int count);

  /**
   * Submit the Cache's context and task on the allocated evaluator
   */
  void submitContextAndTask(AllocatedEvaluator allocatedEvaluator);

  /**
   * Add the running task to be managed
   */
  public boolean addRunningTask(RunningTask task);

  /**
   * Remove the task from the manager
   */
  public void removeRunningTask(String taskId);

  /**
   * Return a view of running Caches. The returned list
   * is a copy -- it does not change as caches get updated.
   */
  public List<CacheNode> getCaches();

  /**
   * Get the cache running at the specified task
   */
  CacheNode getCache(String taskId);

  /**
   * Pass a cache status update to the cache manager
   */
  public void handleHeartbeat(String taskId, CacheStatusMessage status);
}
