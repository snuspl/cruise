package org.apache.reef.inmemory.driver;

import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.inmemory.common.CacheStatusMessage;
import org.apache.reef.inmemory.common.DfsParameters;
import org.apache.reef.inmemory.common.Instrumentor;
import org.apache.reef.inmemory.task.CacheParameters;
import org.apache.reef.inmemory.task.InMemoryTask;
import org.apache.reef.inmemory.task.InMemoryTaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.wake.StageConfiguration;

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
public final class CacheNodeManagerImpl implements CacheNodeManager {

  private static final Logger LOG = Logger.getLogger(CacheNodeManagerImpl.class.getName());

  private final EvaluatorRequestor evaluatorRequestor;
  private final String dfsType;
  private final int cachePort;
  private final int cacheMemory;
  private final int cacheServerThreads;
  private final double cacheHeapSlack;
  private final int cacheLoadingThreads;
  private final Instrumentor instrumentor;

  // Tasks are first added to pendingTasks, then moved to tasks after receiving the server port
  private final Map<String, RunningTask> pendingTasks = new HashMap<>();
  private final Map<String, CacheNode> caches = new HashMap<>();

  @Inject
  public CacheNodeManagerImpl(final EvaluatorRequestor evaluatorRequestor,
                              @Parameter(DfsParameters.Type.class) final String dfsType,
                              @Parameter(CacheParameters.Port.class) final int cachePort,
                              @Parameter(CacheParameters.Memory.class) final int cacheMemory,
                              @Parameter(CacheParameters.NumServerThreads.class) final int cacheServerThreads,
                              @Parameter(CacheParameters.HeapSlack.class) final double cacheHeapSlack,
                              @Parameter(StageConfiguration.NumberOfThreads.class) final int cacheLoadingThreads,
                              final Instrumentor instrumentor) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.dfsType = dfsType;
    this.cachePort = cachePort;
    this.cacheMemory = cacheMemory;
    this.cacheServerThreads = cacheServerThreads;
    this.cacheHeapSlack = cacheHeapSlack;
    this.cacheLoadingThreads = cacheLoadingThreads;
    this.instrumentor = instrumentor;
  }

  /**
   * Get a Task Configuration.
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
              .set(InMemoryTaskConfiguration.CACHESERVER_HEAP_SLACK, cacheHeapSlack)
              .build();
      final Configuration instrumentationConf = instrumentor.getConfiguration();
      final Configuration mergedConf = Configurations.merge(taskConf, taskInMemoryConf, instrumentationConf);
      allocatedEvaluator.submitContextAndTask(contextConf, mergedConf);
    } catch (final BindException ex) {
      LOG.log(Level.SEVERE, "Failed to bind Task.", ex);
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
  public synchronized void handleHeartbeat(final String taskId, final CacheStatusMessage msg) {
    // TODO: eventually, the task manager should remove Caches that have not been heard from for a long time
    if (pendingTasks.containsKey(taskId) && msg.getBindPort() != 0) {
      final RunningTask task = pendingTasks.remove(taskId);
      final CacheNode cache = new CacheNode(task, msg.getBindPort());
      cache.setLatestStatistics(msg.getStatistics());
      cache.setLatestTimestamp(System.currentTimeMillis());
      caches.put(taskId, cache);
      LOG.log(Level.INFO, "Cache "+cache.getAddress()+" added from task "+cache.getTaskId());
    } else if (caches.containsKey(taskId)) {
      final CacheNode cache = caches.get(taskId);
      cache.setLatestStatistics(msg.getStatistics());
      cache.setLatestTimestamp(System.currentTimeMillis());
      caches.put(taskId, cache);
    }
  }
}
