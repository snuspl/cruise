package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.service.MetaServerParameters;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsCacheManager {

  private static final ObjectSerializableCodec<BlockInfo> CODEC = new ObjectSerializableCodec<>();

  private final int numReplicas;
  private final Map<String, RunningTask> tasks;

  @Inject
  public HdfsCacheManager(final @Parameter(MetaServerParameters.Replicas.class) int numReplicas) {
    this.numReplicas = numReplicas;
    this.tasks = new HashMap<String, RunningTask>();
  }

  public boolean addRunningTask(RunningTask task) {
    if (tasks.containsKey(task.getId())) {
      return false;
    } else {
      tasks.put(task.getId(), task);
      return true;
    }
  }

  public void removeRunningTask(String taskId) {
    tasks.remove(taskId);
  }

  public List<RunningTask> getTasksToCache(final LocatedBlock block) {
    final List<RunningTask> tasksToCache = new ArrayList<>(numReplicas);
    int replicasAdded = 0;
    for (RunningTask task : tasks.values()) {
      if (replicasAdded >= numReplicas) break;
      tasksToCache.add(task);
    }
    return tasksToCache;
  }

  public String getCacheHost(final RunningTask task) {
    return task.getActiveContext().getEvaluatorDescriptor()
            .getNodeDescriptor().getInetSocketAddress().getHostString();
  }

  public void sendToTask(RunningTask task, BlockInfo block) {
    task.send(CODEC.encode(block));
  }
}
