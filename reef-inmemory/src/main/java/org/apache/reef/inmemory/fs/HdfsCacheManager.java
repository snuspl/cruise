package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.cache.CacheClearMessage;
import org.apache.reef.inmemory.cache.CacheParameters;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsMessage;
import org.apache.reef.inmemory.fs.service.MetaServerParameters;

import javax.inject.Inject;
import java.util.*;

/**
 * Implements HDFS-specific
 * general Task/Cache management methods,
 */
public final class HdfsCacheManager implements TaskManager {

  private static final ObjectSerializableCodec<HdfsMessage> CODEC = new ObjectSerializableCodec<>();

  private final HdfsTaskSelectionPolicy taskSelectionPolicy;
  private final int cachePort;
  private final Map<String, RunningTask> tasks = new HashMap<>();

  @Inject
  public HdfsCacheManager(final HdfsTaskSelectionPolicy taskSelectionPolicy,
                          final @Parameter(CacheParameters.Port.class) int cachePort) {
    this.taskSelectionPolicy = taskSelectionPolicy;
    this.cachePort = cachePort;
  }

  @Override
  public synchronized boolean addRunningTask(RunningTask task) {
    if (tasks.containsKey(task.getId())) {
      return false;
    } else {
      tasks.put(task.getId(), task);
      return true;
    }
  }

  @Override
  public synchronized void removeRunningTask(String taskId) {
    tasks.remove(taskId);
  }

  @Override
  public synchronized void clearCaches() {
    for (RunningTask task : tasks.values()) {
      task.send(CODEC.encode(new HdfsMessage(new CacheClearMessage())));
    }
  }

  @Override
  public String getCacheAddress(final RunningTask task) {
    return getCacheHost(task) + ":" + cachePort;
  }

  private String getCacheHost(final RunningTask task) {
    return task.getActiveContext().getEvaluatorDescriptor()
            .getNodeDescriptor().getInetSocketAddress().getHostString();
  }

  /**
   * Return the Tasks to place cache replicas on.
   * The current implementation just returns a number of numReplicas Tasks
   * (based on placement within a HashMap).
   * Any future implementations that require more logic should consider whether
   * this synchronized method does not block other methods.
   */
  public synchronized List<RunningTask> getTasksToCache(final LocatedBlock block) {
    return taskSelectionPolicy.select(block, Collections.unmodifiableCollection(tasks.values()));

  }

  public void sendToTask(RunningTask task, HdfsBlockMessage blockMsg) {
    task.send(CODEC.encode(new HdfsMessage(blockMsg)));
  }
}
