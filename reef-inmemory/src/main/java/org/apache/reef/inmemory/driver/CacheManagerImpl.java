package org.apache.reef.inmemory.driver;

import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.StageConfiguration;
import org.apache.reef.inmemory.common.DfsParameters;
import org.apache.reef.inmemory.task.InMemoryTask;
import org.apache.reef.inmemory.task.InMemoryTaskConfiguration;
import org.apache.reef.inmemory.task.CacheParameters;
import org.apache.reef.inmemory.common.CacheStatusMessage;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides an implementation for CacheManager. Messaging is taken care of
 * at CacheMessenger, because it must be implemented per Base FS.
 */
public final class CacheManagerImpl implements CacheManager {

  private static final Logger LOG = Logger.getLogger(CacheManagerImpl.class.getName());

  private final EvaluatorRequestor evaluatorRequestor;
  private final String dfsType;
  private final int cachePort;
  private final int cacheMemory;
  private final int cacheServerThreads;
  private final int cacheLoadingThreads;

  // Tasks are first added to pendingTasks, then moved to tasks after receiving the server port
  private final Map<String, RunningTask> pendingTasks = new HashMap<>();
  private final Map<String, CacheNode> caches = new HashMap<>();

  @Inject
  public CacheManagerImpl(final EvaluatorRequestor evaluatorRequestor,
                          final @Parameter(DfsParameters.Type.class) String dfsType,
                          final @Parameter(CacheParameters.Port.class) int cachePort,
                          final @Parameter(CacheParameters.Memory.class) int cacheMemory,
                          final @Parameter(CacheParameters.NumServerThreads.class) int cacheServerThreads,
                          final @Parameter(StageConfiguration.NumberOfThreads.class) int cacheLoadingThreads) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.dfsType = dfsType;
    this.cachePort = cachePort;
    this.cacheMemory = cacheMemory;
    this.cacheServerThreads = cacheServerThreads;
    this.cacheLoadingThreads = cacheLoadingThreads;
  }

  /**
   * Get a Task Configuration
   */
  protected static Configuration getTaskConfiguration(String uniqueId) throws BindException {
    return TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "InMemoryTask-"+uniqueId)
            .set(TaskConfiguration.TASK, InMemoryTask.class)
            .set(TaskConfiguration.ON_TASK_STARTED, InMemoryTask.StartHandler.class)
            .set(TaskConfiguration.ON_MESSAGE, InMemoryTask.DelegatingDriverMessageHandler.class)
            .set(TaskConfiguration.ON_SEND_MESSAGE, InMemoryTask.class)
            .build();
  }

  @Override
  public synchronized void requestEvaluator(final int count, final int memory) {
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
            .setNumber(count)
            .setMemory(memory)
            .build());
  }

  @Override
  public synchronized void requestEvaluator(final int count) {
    requestEvaluator(count, cacheMemory);
  }

  @Override
  public synchronized void submitContextAndTask(final AllocatedEvaluator allocatedEvaluator) {
    try {
      final Configuration contextConf = ContextConfiguration.CONF
              .set(ContextConfiguration.IDENTIFIER, "InMemoryContext-"+allocatedEvaluator.getId())

              .build();
      final Configuration taskConf = getTaskConfiguration(allocatedEvaluator.getId());
      final Configuration taskInMemoryConf = InMemoryTaskConfiguration.getConf(dfsType)
              .set(InMemoryTaskConfiguration.CACHESERVER_PORT, cachePort)
              .set(InMemoryTaskConfiguration.CACHESERVER_SERVER_THREADS, cacheServerThreads)
              .set(InMemoryTaskConfiguration.CACHESERVER_LOADING_THREADS, cacheLoadingThreads)
              .build();

      allocatedEvaluator.submitContextAndTask(contextConf,
              Tang.Factory.getTang().newConfigurationBuilder(taskConf, taskInMemoryConf).build());
    } catch (final BindException ex) {
      final String message = "Failed to bind Task.";
      LOG.log(Level.SEVERE, message);
    }
  }

  @Override
  public synchronized boolean addRunningTask(final RunningTask task) {
    if (caches.containsKey(task.getId()) || pendingTasks.containsKey(task.getId())) {
      return false;
    } else {
      pendingTasks.put(task.getId(), task);
      LOG.log(Level.INFO, "Add pending task "+task.getId());
      return true;
    }
  }

  @Override
  public synchronized void removeRunningTask(final String taskId) {
    caches.remove(taskId);
  }

  @Override
  public synchronized List<CacheNode> getCaches() {
    return new ArrayList<>(caches.values());
  }

  @Override
  public synchronized CacheNode getCache(final String taskId) {
    return caches.get(taskId);
  }

  @Override
  public synchronized void handleUpdate(final String taskId, final CacheStatusMessage msg) {
    // TODO: eventually, the task manager should remove Caches that have not been heard from for a long time
    if (pendingTasks.containsKey(taskId) && msg.getBindPort() != 0) {
      final RunningTask task = pendingTasks.remove(taskId);
      final CacheNode cache = new CacheNode(task, msg.getBindPort());
      caches.put(taskId, cache);
      LOG.log(Level.INFO, "Cache "+cache.getAddress()+" added from task "+cache.getTaskId());
    } else if (caches.containsKey(taskId)) {
      caches.get(taskId).setLatestStatistics(msg.getStatistics());
    }
  }
}
