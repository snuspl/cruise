package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;

public interface TaskManager {
  public boolean addRunningTask(RunningTask task);
  public void removeRunningTask(String taskId);

  public void clearCaches();
}
