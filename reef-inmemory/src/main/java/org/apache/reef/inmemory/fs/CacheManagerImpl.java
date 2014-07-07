package org.apache.reef.inmemory.fs;

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
import org.apache.reef.inmemory.InMemoryTask;
import org.apache.reef.inmemory.InMemoryTaskConfiguration;
import org.apache.reef.inmemory.cache.CacheParameters;
import org.apache.reef.inmemory.cache.CacheStatusMessage;

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
public class CacheManagerImpl implements CacheManager {

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
  protected static final Configuration getTaskConfiguration() throws BindException {
    return TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "InMemoryTask")
            .set(TaskConfiguration.TASK, InMemoryTask.class)
            .set(TaskConfiguration.ON_TASK_STARTED, InMemoryTask.StartHandler.class)
            .set(TaskConfiguration.ON_MESSAGE, InMemoryTask.DriverMessageHandler.class)
            .set(TaskConfiguration.ON_SEND_MESSAGE, InMemoryTask.class)
            .build();
  }

  @Override
  public final synchronized void requestEvaluator(int count, int memory) {
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
            .setNumber(count)
            .setMemory(memory)
            .build());
  }

  @Override
  public final synchronized void requestEvaluator(int count) {
    requestEvaluator(count, cacheMemory);
  }

  @Override
  public final synchronized void submitContextAndTask(final AllocatedEvaluator allocatedEvaluator) {
    try {
      final Configuration contextConf = ContextConfiguration.CONF
              .set(ContextConfiguration.IDENTIFIER, "InMemoryContext")
              .build();
      final Configuration taskConf = getTaskConfiguration();
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
      throw new RuntimeException(message, ex);
    }
  }

  @Override
  public final synchronized boolean addRunningTask(RunningTask task) {
    if (caches.containsKey(task.getId()) || pendingTasks.containsKey(task.getId())) {
      return false;
    } else {
      pendingTasks.put(task.getId(), task);
      LOG.log(Level.INFO, "Add pending task "+task.getId());
      return true;
    }
  }

  @Override
  public final synchronized void removeRunningTask(String taskId) {
    caches.remove(taskId);
  }

  @Override
  public final synchronized List<CacheNode> getCaches() {
    return new ArrayList<>(caches.values());
  }

  @Override
  public final synchronized CacheNode getCache(String taskId) {
    return caches.get(taskId);
  }

  @Override
  public final synchronized void handleUpdate(String taskId, CacheStatusMessage msg) {
    // TODO: eventually, the cache manager should remove Caches that have not been heard from for a long time
    if (pendingTasks.containsKey(taskId) && msg.getBindPort() != 0) {
      final RunningTask task = pendingTasks.remove(taskId);
      final CacheNode cache = new CacheNode(task, msg.getBindPort());
      caches.put(taskId, cache);
      LOG.log(Level.INFO, "Cache "+cache.getAddress()+" added from task "+cache.getTaskId());
    }
  }
}
