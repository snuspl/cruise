package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.cache.CacheClearMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsMessage;
import org.apache.reef.inmemory.fs.service.MetaServerParameters;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class HdfsCacheManager implements TaskManager {

  private static final ObjectSerializableCodec<HdfsMessage> CODEC = new ObjectSerializableCodec<>();

  private final int numReplicas;
  private final Map<String, RunningTask> tasks = new HashMap<>();

  @Inject
  public HdfsCacheManager(final @Parameter(MetaServerParameters.Replicas.class) int numReplicas) {
    this.numReplicas = numReplicas;
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

  /**
   * Return the Tasks to place cache replicas on.
   * The current implementation just returns a number of numReplicas Tasks
   * (based on placement within a HashMap).
   * Any future implementations that require more logic should consider whether
   * this synchronized method does not block other methods.
   */
  public synchronized List<RunningTask> getTasksToCache(final LocatedBlock block) {
    final List<RunningTask> tasksToCache = new ArrayList<>(numReplicas);
    int replicasAdded = 0;
    for (RunningTask task : tasks.values()) {
      if (replicasAdded >= numReplicas) break;
      tasksToCache.add(task);
      replicasAdded++;
    }
    return tasksToCache;
  }

  public String getCacheHost(final RunningTask task) {
    return task.getActiveContext().getEvaluatorDescriptor()
            .getNodeDescriptor().getInetSocketAddress().getHostString();
  }

  public void sendToTask(RunningTask task, HdfsBlockMessage blockMsg) {
    task.send(CODEC.encode(new HdfsMessage(blockMsg)));
  }
}
