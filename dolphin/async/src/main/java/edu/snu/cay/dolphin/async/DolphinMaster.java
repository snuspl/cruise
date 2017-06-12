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

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import edu.snu.cay.dolphin.async.metric.ETDolphinMetricMsgCodec;
import edu.snu.cay.dolphin.async.metric.parameters.ServerMetricFlushPeriodMs;
import edu.snu.cay.dolphin.async.network.NetworkConnection;
import edu.snu.cay.dolphin.async.network.NetworkConfProvider;
import edu.snu.cay.dolphin.async.optimizer.impl.ETOptimizationOrchestrator;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.RemoteAccessConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.cay.services.et.configuration.parameters.ValueCodec;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.TaskResult;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import edu.snu.cay.services.et.metric.MetricManager;
import edu.snu.cay.services.et.metric.configuration.MetricServiceExecutorConf;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.ETServerTask.SERVER_TASK_ID_PREFIX;
import static edu.snu.cay.dolphin.async.ETModelAccessor.MODEL_TABLE_ID;
import static edu.snu.cay.dolphin.async.ETTrainingDataProvider.TRAINING_DATA_TABLE_ID;
import static edu.snu.cay.dolphin.async.ETWorkerTask.TASK_ID_PREFIX;

/**
 * A Dolphin master using ET.
 */
public final class DolphinMaster {
  private static final Logger LOG = Logger.getLogger(DolphinMaster.class.getName());

  private final ETMaster etMaster;
  private final MetricManager metricManager;
  private final ETTaskRunner taskRunner;
  private final ProgressTracker progressTracker;
  private final MasterSideMsgHandler msgHandler;

  private final int numWorkers;
  private final int numServers;
  private final long serverMetricFlushPeriodMs;

  private final Configuration workerConf;

  private final ResourceConfiguration workerResourceConf;
  private final ResourceConfiguration serverResourceConf;

  private final RemoteAccessConfiguration workerRemoteAccessConf;
  private final RemoteAccessConfiguration serverRemoteAccessConf;

  private final Configuration executorContextConf;
  private final Configuration executorServiceConf;

  private final TableConfiguration workerTableConf;
  private final TableConfiguration serverTableConf;
  private final String inputPath;

  private final AtomicInteger workerTaskIdCount = new AtomicInteger(0);
  private final AtomicInteger serverTaskIdCount = new AtomicInteger(0);

  @Inject
  private DolphinMaster(final ETMaster etMaster,
                        final MetricManager metricManager,
                        final ETOptimizationOrchestrator optimizationOrchestrator,
                        final ETTaskRunner taskRunner,
                        final ProgressTracker progressTracker,
                        final ConfigurationSerializer confSerializer,
                        final NetworkConfProvider networkConfProvider,
                        final NetworkConnection<DolphinMsg> networkConnection,
                        final MasterSideMsgHandler masterSideMsgHandler,
                        @Parameter(NumServers.class) final int numServers,
                        @Parameter(ServerMemSize.class) final int serverMemSize,
                        @Parameter(NumServerCores.class) final int numServerCores,
                        @Parameter(NumWorkers.class) final int numWorkers,
                        @Parameter(WorkerMemSize.class) final int workerMemSize,
                        @Parameter(NumWorkerCores.class) final int numWorkerCores,
                        @Parameter(NumServerSenderThreads.class) final int numServerSenderThreads,
                        @Parameter(NumServerHandlerThreads.class) final int numServerHandlerThreads,
                        @Parameter(ServerSenderQueueSize.class) final int serverSenderQueueSize,
                        @Parameter(ServerHandlerQueueSize.class) final int serverHandlerQueueSize,
                        @Parameter(NumWorkerSenderThreads.class) final int numWorkerSenderThreads,
                        @Parameter(NumWorkerHandlerThreads.class) final int numWorkerHandlerThreads,
                        @Parameter(WorkerSenderQueueSize.class) final int workerSenderQueueSize,
                        @Parameter(WorkerHandlerQueueSize.class) final int workerHandlerQueueSize,
                        @Parameter(NumWorkerBlocks.class) final int numWorkerBlocks,
                        @Parameter(NumServerBlocks.class) final int numServerBlocks,
                        @Parameter(ServerMetricFlushPeriodMs.class) final long serverMetricFlushPeriodMs,
                        @Parameter(JobIdentifier.class) final String jobId,
                        @Parameter(ETDolphinLauncher.SerializedParamConf.class) final String serializedParamConf,
                        @Parameter(ETDolphinLauncher.SerializedWorkerConf.class) final String serializedWorkerConf,
                        @Parameter(ETDolphinLauncher.SerializedServerConf.class) final String serializedServerConf)
      throws IOException, InjectionException {
    this.etMaster = etMaster;
    this.metricManager = metricManager;
    this.taskRunner = taskRunner;
    this.progressTracker = progressTracker;
    this.msgHandler = masterSideMsgHandler;

    this.numWorkers = numWorkers;
    this.numServers = numServers;
    this.serverMetricFlushPeriodMs = serverMetricFlushPeriodMs;

    // configuration commonly used in both workers and servers
    final Configuration userParamConf = confSerializer.fromString(serializedParamConf);

    // initialize server-side configuration
    final Configuration serverConf = confSerializer.fromString(serializedServerConf);
    final Injector serverInjector = Tang.Factory.getTang().newInjector(serverConf);
    this.serverResourceConf = buildResourceConf(numServerCores, serverMemSize);
    this.serverRemoteAccessConf = buildRemoteAccessConf(numServerSenderThreads, serverSenderQueueSize,
        numServerHandlerThreads, serverHandlerQueueSize);
    this.serverTableConf = buildServerTableConf(serverInjector, numServerBlocks, userParamConf);

    // initialize worker-side configurations
    this.workerConf = confSerializer.fromString(serializedWorkerConf);
    final Injector workerInjector = Tang.Factory.getTang().newInjector(workerConf);
    this.workerResourceConf = buildResourceConf(numWorkerCores, workerMemSize);
    this.workerRemoteAccessConf = buildRemoteAccessConf(numWorkerSenderThreads, workerSenderQueueSize,
        numWorkerHandlerThreads, workerHandlerQueueSize);
    this.workerTableConf = buildWorkerTableConf(workerInjector, numWorkerBlocks, userParamConf);
    this.inputPath = workerInjector.getNamedInstance(Parameters.InputDir.class);

    // network configuration for executors
    this.executorContextConf = networkConfProvider.getContextConfiguration();
    this.executorServiceConf = networkConfProvider.getServiceConfiguration(jobId);

    // register master with job id
    networkConnection.setup(jobId, jobId);

    optimizationOrchestrator.start();
  }

  private static ResourceConfiguration buildResourceConf(final int numCores, final int memSize) {
    return ResourceConfiguration.newBuilder()
        .setNumCores(numCores)
        .setMemSizeInMB(memSize)
        .build();
  }

  private static RemoteAccessConfiguration buildRemoteAccessConf(final int numSenderThreads,
                                                                 final int senderQueueSize,
                                                                 final int numHandlerThreads,
                                                                 final int handlerQueueSize) {
    return RemoteAccessConfiguration.newBuilder()
        .setNumSenderThreads(numSenderThreads)
        .setSenderQueueSize(senderQueueSize)
        .setNumHandlerThreads(numHandlerThreads)
        .setHandlerQueueSize(handlerQueueSize)
        .build();
  }

  private static TableConfiguration buildWorkerTableConf(final Injector workerInjector,
                                                         final int numTotalBlocks,
                                                         final Configuration userParamConf) throws InjectionException {
    final Codec keyCodec = workerInjector.getNamedInstance(KeyCodec.class);
    final Codec valueCodec = workerInjector.getNamedInstance(ValueCodec.class);
    final DataParser dataParser = workerInjector.getInstance(DataParser.class);

    return TableConfiguration.newBuilder()
        .setId(TRAINING_DATA_TABLE_ID)
        .setKeyCodecClass(keyCodec.getClass())
        .setValueCodecClass(valueCodec.getClass())
        .setUpdateValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setNumTotalBlocks(numTotalBlocks)
        .setIsMutableTable(false)
        .setIsOrderedTable(true)
        .setDataParserClass(dataParser.getClass())
        .setUserParamConf(userParamConf)
        .build();
  }

  private static TableConfiguration buildServerTableConf(final Injector serverInjector,
                                                         final int numTotalBlocks,
                                                         final Configuration userParamConf) throws InjectionException {
    final Codec keyCodec = serverInjector.getNamedInstance(KeyCodec.class);
    final Codec valueCodec = serverInjector.getNamedInstance(ValueCodec.class);
    final Codec updateValueCodec = serverInjector.getNamedInstance(UpdateValueCodec.class);
    final UpdateFunction updateFunction = serverInjector.getInstance(UpdateFunction.class);

    return TableConfiguration.newBuilder()
        .setId(MODEL_TABLE_ID)
        .setKeyCodecClass(keyCodec.getClass())
        .setValueCodecClass(valueCodec.getClass())
        .setUpdateValueCodecClass(updateValueCodec.getClass())
        .setUpdateFunctionClass(updateFunction.getClass())
        .setNumTotalBlocks(numTotalBlocks)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .setUserParamConf(userParamConf)
        .build();
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

  public ExecutorConfiguration getWorkerExecutorConf() {
    return ExecutorConfiguration.newBuilder()
        .setResourceConf(workerResourceConf)
        .setRemoteAccessConf(workerRemoteAccessConf)
        .setUserContextConf(executorContextConf)
        .setUserServiceConf(executorServiceConf)
        .build();
  }

  public ExecutorConfiguration getServerExecutorConf() {
    return ExecutorConfiguration.newBuilder()
        .setResourceConf(serverResourceConf)
        .setRemoteAccessConf(serverRemoteAccessConf)
        .setUserContextConf(executorContextConf)
        .setUserServiceConf(executorServiceConf)
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
   * Start running a job.
   */
  public void start() {
    try {
      final List<AllocatedExecutor> servers = etMaster.addExecutors(numServers, getServerExecutorConf()).get();
      servers.forEach(server -> metricManager.startMetricCollection(server.getId(), getServerMetricConf()));

      final List<AllocatedExecutor> workers = etMaster.addExecutors(numWorkers, getWorkerExecutorConf()).get();
      workers.forEach(worker -> metricManager.startMetricCollection(worker.getId(), getWorkerMetricConf()));

      Executors.newSingleThreadExecutor().submit(() -> {
        try {
          final Future<AllocatedTable> modelTable = etMaster.createTable(serverTableConf, servers);
          final Future<AllocatedTable> inputTable = etMaster.createTable(workerTableConf, workers);

          modelTable.get().subscribe(workers);
          inputTable.get().load(workers, inputPath).get();

          final List<TaskResult> taskResults = taskRunner.run(workers, servers);
          checkTaskResults(taskResults);

          workers.forEach(AllocatedExecutor::close);
          servers.forEach(AllocatedExecutor::close);
        } catch (Exception e) {
          LOG.log(Level.SEVERE, "Exception while running a job", e);
          throw new RuntimeException(e);
        }
      });

    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
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
