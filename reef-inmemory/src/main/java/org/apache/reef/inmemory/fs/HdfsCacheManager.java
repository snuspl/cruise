package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.commons.collections.Unmodifiable;
import org.apache.commons.collections.collection.UnmodifiableCollection;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.cache.CacheClearMessage;
import org.apache.reef.inmemory.cache.CacheParameters;
import org.apache.reef.inmemory.cache.CacheStatusMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsMessage;
import org.apache.reef.inmemory.fs.service.MetaServerParameters;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements HDFS-specific
 * general Task/Cache management methods,
 */
public final class HdfsCacheManager implements TaskManager {

  private static final Logger LOG = Logger.getLogger(HdfsCacheManager.class.getName());

  private static final ObjectSerializableCodec<HdfsMessage> CODEC = new ObjectSerializableCodec<>();

  private final HdfsTaskSelectionPolicy taskSelectionPolicy;

  // Tasks are first added to pendingTasks, then moved to tasks after receiving the server port
  private final Map<String, RunningTask> tasks = new HashMap<>();
  private final Map<String, RunningTask> pendingTasks = new HashMap<>();
  private final Map<String, Integer> cachePorts = new HashMap<>();

  @Inject
  public HdfsCacheManager(final HdfsTaskSelectionPolicy taskSelectionPolicy) {
    this.taskSelectionPolicy = taskSelectionPolicy;
  }

  @Override
  public synchronized boolean addRunningTask(RunningTask task) {
    if (tasks.containsKey(task.getId()) || pendingTasks.containsKey(task.getId())) {
      return false;
    } else {
      pendingTasks.put(task.getId(), task);
      LOG.log(Level.INFO, "Add pending task "+task.getId()+" at host "+getCacheHost(task));
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
   * Gets the Cache server's address
   * This will return the node's address as defined by Wake, based on its network interfaces,
   * so it can be contacted remotely. This means that you will not see localhost/127.0.0.1
   * as the hostname even on local deployments.
   * @See com.microsoft.wake.remote.NetUtils.getLocalAddress()
   */
  @Override
  public synchronized String getCacheAddress(final String taskId) throws IOException {
    if (cachePorts.containsKey(taskId)) {
      return getCacheHost(tasks.get(taskId)) + ":" + cachePorts.get(taskId);
    } else {
      throw new IOException("Cache port is unknown, may not have started.");
    }
  }

  private static String getCacheHost(final RunningTask task) {
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
  public List<RunningTask> getTasksToCache(final LocatedBlock block) {
    final List<RunningTask> taskList;
    synchronized (this) {
      taskList = new ArrayList<>(tasks.values());
    }
    return taskSelectionPolicy.select(block, taskList);
  }

  public void sendToTask(RunningTask task, HdfsBlockMessage blockMsg) {
    task.send(CODEC.encode(new HdfsMessage(blockMsg)));
  }

  @Override
  public synchronized void handleUpdate(String taskId, CacheStatusMessage msg) {
    // TODO: eventually, the cache manager should remove Caches that have not been heard from for a long time
    if (pendingTasks.containsKey(taskId) && msg.getBindPort() != 0) {
      cachePorts.put(taskId, msg.getBindPort());
      RunningTask task = pendingTasks.remove(taskId);
      tasks.put(taskId, task);
      LOG.log(Level.INFO, "Add bindPort "+msg.getBindPort()+" to "+getCacheHost(tasks.get(taskId)));
    }
  }
}
