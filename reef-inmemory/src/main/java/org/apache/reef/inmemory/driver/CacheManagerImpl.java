package org.apache.reef.inmemory.driver;

import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.inmemory.common.instrumentation.InstrumentationConfiguration;
import org.apache.reef.inmemory.common.instrumentation.InstrumentationParameters;
import org.apache.reef.inmemory.common.instrumentation.ganglia.GangliaConfiguration;
import org.apache.reef.inmemory.common.instrumentation.ganglia.GangliaParameters;
import org.apache.reef.inmemory.common.instrumentation.log.LogReporterConstructor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.wake.StageConfiguration;
import org.apache.reef.inmemory.common.CacheStatusMessage;
import org.apache.reef.inmemory.common.DfsParameters;
import org.apache.reef.inmemory.task.CacheParameters;
import org.apache.reef.inmemory.task.InMemoryTask;
import org.apache.reef.inmemory.task.InMemoryTaskConfiguration;

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
  private final double cacheHeapSlack;
  private final int cacheLoadingThreads;
  private final int reporterPeriod;
  private final String reporterLogLevel;
  private final boolean ganglia;
  private final String gangliaHost;
  private final int gangliaPort;
  private final String gangliaPrefix;

  // Tasks are first added to pendingTasks, then moved to tasks after receiving the server port
  private final Map<String, RunningTask> pendingTasks = new HashMap<>();
  private final Map<String, CacheNode> caches = new HashMap<>();

  @Inject
  public CacheManagerImpl(final EvaluatorRequestor evaluatorRequestor,
                          final @Parameter(DfsParameters.Type.class) String dfsType,
                          final @Parameter(CacheParameters.Port.class) int cachePort,
                          final @Parameter(CacheParameters.Memory.class) int cacheMemory,
                          final @Parameter(CacheParameters.NumServerThreads.class) int cacheServerThreads,
                          final @Parameter(CacheParameters.HeapSlack.class) double cacheHeapSlack,
                          final @Parameter(StageConfiguration.NumberOfThreads.class) int cacheLoadingThreads,
                          final @Parameter(InstrumentationParameters.InstrumentationReporterPeriod.class) int reporterPeriod,
                          final @Parameter(InstrumentationParameters.InstrumentationLogLevel.class) String reporterLogLevel,
                          final @Parameter(GangliaParameters.Ganglia.class) boolean ganglia,
                          final @Parameter(GangliaParameters.GangliaHost.class) String gangliaHost,
                          final @Parameter(GangliaParameters.GangliaPort.class) int gangliaPort,
                          final @Parameter(GangliaParameters.GangliaPrefix.class) String gangliaPrefix) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.dfsType = dfsType;
    this.cachePort = cachePort;
    this.cacheMemory = cacheMemory;
    this.cacheServerThreads = cacheServerThreads;
    this.cacheHeapSlack = cacheHeapSlack;
    this.cacheLoadingThreads = cacheLoadingThreads;
    this.reporterPeriod = reporterPeriod;
    this.reporterLogLevel = reporterLogLevel;
    this.ganglia = ganglia;
    this.gangliaHost = gangliaHost;
    this.gangliaPort = gangliaPort;
    this.gangliaPrefix = gangliaPrefix;
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
              .set(InMemoryTaskConfiguration.CACHESERVER_HEAP_SLACK, cacheHeapSlack)
              .build();
      final Configuration instrumentationConf = InstrumentationConfiguration.CONF
              .set(InstrumentationConfiguration.REPORTER_PERIOD, reporterPeriod)
              .set(InstrumentationConfiguration.LOG_LEVEL, reporterLogLevel)
              .set(InstrumentationConfiguration.REPORTER_CONSTRUCTORS, LogReporterConstructor.class)
              .build();

      final Configuration mergedConf;
      if (ganglia) {
        LOG.log(Level.INFO, "Configuring task with Ganglia");
        final Configuration gangliaConf = GangliaConfiguration.CONF
                        .set(GangliaConfiguration.GANGLIA, true)
                        .set(GangliaConfiguration.GANGLIA_HOST, gangliaHost)
                        .set(GangliaConfiguration.GANGLIA_PORT, gangliaPort)
                        .set(GangliaConfiguration.GANGLIA_PREFIX, gangliaPrefix)
                        .build();
        mergedConf = Tang.Factory.getTang().newConfigurationBuilder(
                        taskConf, taskInMemoryConf, instrumentationConf, gangliaConf).build();
      } else {
        LOG.log(Level.INFO, "Configuring task without Ganglia");
        mergedConf = Tang.Factory.getTang().newConfigurationBuilder(
                        taskConf, taskInMemoryConf, instrumentationConf).build();
      }
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
      caches.put(taskId, cache);
      LOG.log(Level.INFO, "Cache "+cache.getAddress()+" added from task "+cache.getTaskId());
    } else if (caches.containsKey(taskId)) {
      final CacheNode cache = caches.get(taskId);
      cache.setLatestStatistics(msg.getStatistics());
      cache.setLatestTimestamp(System.currentTimeMillis());
    }
  }
}
