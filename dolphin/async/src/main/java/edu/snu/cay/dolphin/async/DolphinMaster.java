/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.dolphin.async;

import edu.snu.cay.dolphin.async.DolphinParameters.*;
import edu.snu.cay.dolphin.async.metric.ETDolphinMetricMsgCodec;
import edu.snu.cay.dolphin.async.metric.parameters.ServerMetricFlushPeriodMs;
import edu.snu.cay.dolphin.async.network.NetworkConfProvider;
import edu.snu.cay.dolphin.async.network.NetworkConnection;
import edu.snu.cay.dolphin.async.optimizer.impl.ETOptimizationOrchestrator;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.TaskResult;
import edu.snu.cay.services.et.metric.MetricManager;
import edu.snu.cay.services.et.metric.configuration.MetricServiceExecutorConf;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.ETServerTask.SERVER_TASK_ID_PREFIX;
import static edu.snu.cay.dolphin.async.ETWorkerTask.TASK_ID_PREFIX;

/**
 * A Dolphin master, which runs a dolphin job with given executors and tables.
 */
public final class DolphinMaster {
  private static final Logger LOG = Logger.getLogger(DolphinMaster.class.getName());

  private final MetricManager metricManager;
  private final ETTaskRunner taskRunner;
  private final ProgressTracker progressTracker;
  private final MasterSideMsgHandler msgHandler;

  private final long serverMetricFlushPeriodMs;

  private final Configuration workerConf;

  private final AtomicInteger workerTaskIdCount = new AtomicInteger(0);
  private final AtomicInteger serverTaskIdCount = new AtomicInteger(0);

  @Inject
  private DolphinMaster(final MetricManager metricManager,
                        final ETOptimizationOrchestrator optimizationOrchestrator,
                        final ETTaskRunner taskRunner,
                        final ProgressTracker progressTracker,
                        final ConfigurationSerializer confSerializer,
                        final NetworkConfProvider networkConfProvider,
                        final NetworkConnection<DolphinMsg> networkConnection,
                        final MasterSideMsgHandler masterSideMsgHandler,
                        @Parameter(JobIdentifier.class) final String jobId,
                        @Parameter(ServerMetricFlushPeriodMs.class) final long serverMetricFlushPeriodMs,
                        @Parameter(ETDolphinLauncher.SerializedWorkerConf.class) final String serializedWorkerConf)
      throws IOException, InjectionException {
    this.metricManager = metricManager;
    this.taskRunner = taskRunner;
    this.progressTracker = progressTracker;
    this.msgHandler = masterSideMsgHandler;
    // register master with job id
    networkConnection.setup(jobId, jobId);
    this.serverMetricFlushPeriodMs = serverMetricFlushPeriodMs;
    this.workerConf = confSerializer.fromString(serializedWorkerConf);
    optimizationOrchestrator.start();
  }

  public Configuration getWorkerTaskConf() {
    return Configurations.merge(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, TASK_ID_PREFIX + workerTaskIdCount.getAndIncrement())
            .set(TaskConfiguration.TASK, ETWorkerTask.class)
            .set(TaskConfiguration.ON_CLOSE, WorkerTaskCloseHandler.class)
            .build(),
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindNamedParameter(StartingEpochIdx.class, Integer.toString(progressTracker.getGlobalMinEpochIdx()))
            .build(),
        workerConf);
  }

  public Configuration getServerTaskConf() {
    return TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, SERVER_TASK_ID_PREFIX + serverTaskIdCount.getAndIncrement())
        .set(TaskConfiguration.TASK, ETServerTask.class)
        .set(TaskConfiguration.ON_CLOSE, ServerTaskCloseHandler.class)
        .build();
  }

  public MetricServiceExecutorConf getWorkerMetricConf() {
    return MetricServiceExecutorConf.newBuilder()
        .setCustomMetricCodec(ETDolphinMetricMsgCodec.class)
        .build();
  }

  public MetricServiceExecutorConf getServerMetricConf() {
    return MetricServiceExecutorConf.newBuilder()
        .setMetricFlushPeriodMs(serverMetricFlushPeriodMs)
        .build();
  }

  public MasterSideMsgHandler getMsgHandler() {
    return msgHandler;
  }

  /**
   * Start running a job with given executors and tables.
   * It returns after checking the result of tasks.
   * TODO #1175: In multi-job mode, each dolphin master will use given tables
   */
  public void start(final List<AllocatedExecutor> servers, final List<AllocatedExecutor> workers,
                    final AllocatedTable modelTable, final AllocatedTable trainingDataTable) {
    try {
      servers.forEach(server -> metricManager.startMetricCollection(server.getId(), getServerMetricConf()));
      workers.forEach(worker -> metricManager.startMetricCollection(worker.getId(), getWorkerMetricConf()));

      final List<TaskResult> taskResults = taskRunner.run(workers, servers);
      checkTaskResults(taskResults);
    } catch (Exception e) {
      throw new RuntimeException("Dolphin job has been failed", e);
    }
  }

  private void checkTaskResults(final List<TaskResult> taskResultList) {
    taskResultList.forEach(taskResult -> {
      if (!taskResult.isSuccess()) {
        final String taskId = taskResult.getFailedTask().get().getId();
        throw new RuntimeException(String.format("Task %s has been failed", taskId));
      }
    });
    LOG.log(Level.INFO, "Worker tasks completes successfully");
  }
}
