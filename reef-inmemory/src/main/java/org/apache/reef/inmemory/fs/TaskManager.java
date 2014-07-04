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
public interface TaskManager {
  public boolean addRunningTask(RunningTask task);
  public void removeRunningTask(String taskId);
  public void clearCaches();
  public String getCacheAddress(String taskId) throws IOException;
  public void handleUpdate(String taskId, CacheStatusMessage status);
}
