package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;

/**
 * Supports FS-agnostic Task Management operations. Implementing classes must
 * support concurrent calls from multiple threads.
 */
public interface TaskManager {
  public boolean addRunningTask(RunningTask task);
  public void removeRunningTask(String taskId);

  public void clearCaches();
}
