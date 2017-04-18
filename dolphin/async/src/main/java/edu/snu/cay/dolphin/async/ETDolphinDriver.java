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

import edu.snu.cay.common.centcomm.master.CentCommConfProvider;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import edu.snu.cay.dolphin.async.metric.ETDolphinMetricMsgCodec;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.configuration.metric.MetricServiceExecutorConf;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.cay.services.et.configuration.parameters.ValueCodec;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.SubmittedTask;
import edu.snu.cay.services.et.driver.impl.TaskResult;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.snu.cay.dolphin.async.ETModelAccessor.MODEL_TABLE_ID;
import static edu.snu.cay.dolphin.async.ETTrainingDataProvider.TRAINING_DATA_TABLE_ID;
import static edu.snu.cay.dolphin.async.ETWorkerTask.TASK_ID_PREFIX;

/**
 * Driver code for Dolphin on ET.
 */
@Unit
public final class ETDolphinDriver {
  private final ETMaster etMaster;

  private final int numWorkers;
  private final int numServers;

  private final Configuration workerConf;

  private final ResourceConfiguration workerResourceConf;
  private final ResourceConfiguration serverResourceConf;

  private final TableConfiguration workerTableConf;
  private final TableConfiguration serverTableConf;

  private final Configuration workerContextConf;
  private final Configuration workerServiceConf;

  private final Configuration serverServiceConf;

  private final AtomicInteger taskIdCount = new AtomicInteger(0);

  @Inject
  private ETDolphinDriver(final ETMaster etMaster,
                          final ConfigurationSerializer confSerializer,
                          final CentCommConfProvider centCommConfProvider,
                          @Parameter(NumServers.class) final int numServers,
                          @Parameter(ServerMemSize.class) final int serverMemSize,
                          @Parameter(NumServerCores.class) final int numServerCores,
                          @Parameter(NumWorkers.class) final int numWorkers,
                          @Parameter(WorkerMemSize.class) final int workerMemSize,
                          @Parameter(NumWorkerCores.class) final int numWorkerCores,
                          @Parameter(ServerMetricFlushPeriodMs.class) final long serverMetricFlushPeriodMs,
                          @Parameter(ETDolphinLauncher.SerializedParamConf.class) final String serializedParamConf,
                          @Parameter(ETDolphinLauncher.SerializedWorkerConf.class) final String serializedWorkerConf,
                          @Parameter(ETDolphinLauncher.SerializedServerConf.class) final String serializedServerConf)
      throws IOException, InjectionException {
    this.etMaster = etMaster;
    this.numWorkers = numWorkers;
    this.numServers = numServers;

    // configuration commonly used in both workers and servers
    final Configuration userParamConf = confSerializer.fromString(serializedParamConf);

    // initialize server-side configurations
    final Configuration serverConf = confSerializer.fromString(serializedServerConf);
    final Injector serverInjector = Tang.Factory.getTang().newInjector(serverConf);
    this.serverResourceConf = buildResourceConf(numServerCores, serverMemSize);
    this.serverTableConf = buildServerTableConf(serverInjector, userParamConf);

    // initialize worker-side configurations
    this.workerConf = confSerializer.fromString(serializedWorkerConf);
    final Injector workerInjector = Tang.Factory.getTang().newInjector(workerConf);
    this.workerResourceConf = buildResourceConf(numWorkerCores, workerMemSize);
    this.workerTableConf = buildWorkerTableConf(workerInjector, userParamConf);

    // user configuration for worker executors
    this.workerContextConf = centCommConfProvider.getContextConfiguration();
    this.workerServiceConf = Configurations.merge(
        centCommConfProvider.getServiceConfWithoutNameResolver(),
        MetricServiceExecutorConf.CONF
            .set(MetricServiceExecutorConf.CUSTOM_METRIC_CODEC, ETDolphinMetricMsgCodec.class)
            .build());

    // user configuration for server executors
    this.serverServiceConf = MetricServiceExecutorConf.CONF
        .set(MetricServiceExecutorConf.METRIC_SENDING_PERIOD_MS, serverMetricFlushPeriodMs)
        .build();
  }

  private static ResourceConfiguration buildResourceConf(final int numCores, final int memSize) {
    return ResourceConfiguration.newBuilder()
        .setNumCores(numCores)
        .setMemSizeInMB(memSize)
        .build();
  }

  private static TableConfiguration buildWorkerTableConf(final Injector workerInjector,
                                                         final Configuration userParamConf) throws InjectionException {
    final Codec keyCodec = workerInjector.getNamedInstance(KeyCodec.class);
    final Codec valueCodec = workerInjector.getNamedInstance(ValueCodec.class);
    final DataParser dataParser = workerInjector.getInstance(DataParser.class);
    final String inputPath = workerInjector.getNamedInstance(Parameters.InputDir.class);

    return TableConfiguration.newBuilder()
        .setId(TRAINING_DATA_TABLE_ID)
        .setKeyCodecClass(keyCodec.getClass())
        .setValueCodecClass(valueCodec.getClass())
        .setUpdateValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setIsOrderedTable(true)
        .setFilePath(inputPath)
        .setDataParserClass(dataParser.getClass())
        .setUserParamConf(userParamConf)
        .build();
  }

  private static TableConfiguration buildServerTableConf(final Injector serverInjector,
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
        .setIsOrderedTable(false)
        .setUserParamConf(userParamConf)
        .build();
  }

  public Configuration getWorkerTaskConf() {
    return Configurations.merge(TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, TASK_ID_PREFIX + taskIdCount.getAndIncrement())
        .set(TaskConfiguration.TASK, ETWorkerTask.class)
        .set(TaskConfiguration.ON_TASK_STOP, ETTaskStopHandler.class)
        .build(), workerConf);
  }

  public ExecutorConfiguration getWorkerExecutorConf() {
    return ExecutorConfiguration.newBuilder()
        .setResourceConf(workerResourceConf)
        .setUserContextConf(workerContextConf)
        .setUserServiceConf(workerServiceConf)
        .build();
  }

  public ExecutorConfiguration getServerExecutorConf() {
    return ExecutorConfiguration.newBuilder()
        .setResourceConf(serverResourceConf)
        .setUserServiceConf(serverServiceConf)
        .build();
  }

  /**
   * A driver start handler for requesting executors.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      try {
        final List<AllocatedExecutor> servers = etMaster.addExecutors(numServers, getServerExecutorConf()).get();
        final List<AllocatedExecutor> workers = etMaster.addExecutors(numWorkers, getWorkerExecutorConf()).get();

        Executors.newSingleThreadExecutor().submit(() -> {
          try {
            final Future<AllocatedTable> modelTable = etMaster.createTable(serverTableConf, servers);
            final Future<AllocatedTable> inputTable = etMaster.createTable(workerTableConf, workers);

            modelTable.get().subscribe(workers);
            inputTable.get();

            final List<Future<SubmittedTask>> taskFutureList = new ArrayList<>(workers.size());
            workers.forEach(worker -> taskFutureList.add(worker.submitTask(getWorkerTaskConf())));

            waitAndCheckTaskResult(taskFutureList);

            workers.forEach(AllocatedExecutor::close);
            servers.forEach(AllocatedExecutor::close);
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });

      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void waitAndCheckTaskResult(final List<Future<SubmittedTask>> taskFutureList) {
    taskFutureList.forEach(taskFuture -> {
      try {
        final TaskResult taskResult = taskFuture.get().getTaskResult();
        if (!taskResult.isSuccess()) {
          final String taskId = taskResult.getFailedTask().get().getId();
          throw new RuntimeException(String.format("Task %s has been failed", taskId));
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
