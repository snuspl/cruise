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

import edu.snu.cay.common.aggregation.driver.AggregationManager;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.cay.services.et.configuration.parameters.ValueCodec;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
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

  private final Configuration aggrContextConf;
  private final Configuration aggrServiceConf;

  @Inject
  private ETDolphinDriver(final ETMaster etMaster,
                          final ConfigurationSerializer confSerializer,
                          final AggregationManager aggregationManager,
                          @Parameter(NumServers.class) final int numServers,
                          @Parameter(ServerMemSize.class) final int serverMemSize,
                          @Parameter(NumServerCores.class) final int numServerCores,
                          @Parameter(NumWorkers.class) final int numWorkers,
                          @Parameter(WorkerMemSize.class) final int workerMemSize,
                          @Parameter(NumWorkerCores.class) final int numWorkerCores,
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

    // configuration for worker executors
    this.aggrContextConf = aggregationManager.getContextConfiguration();
    this.aggrServiceConf = aggregationManager.getServiceConfigurationWithoutNameResolver();
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

  /**
   * A driver start handler for requesting executors.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final ExecutorConfiguration serverExecutorConf = ExecutorConfiguration.newBuilder()
          .setResourceConf(serverResourceConf)
          .build();
      final ExecutorConfiguration workerExecutorConf = ExecutorConfiguration.newBuilder()
          .setResourceConf(workerResourceConf)
          .setUserContextConf(aggrContextConf)
          .setUserServiceConf(aggrServiceConf)
          .build();
      final List<AllocatedExecutor> servers = etMaster.addExecutors(numServers, serverExecutorConf);
      final List<AllocatedExecutor> workers = etMaster.addExecutors(numWorkers, workerExecutorConf);

      Executors.newSingleThreadExecutor().submit(() -> {
        etMaster.createTable(serverTableConf, servers).subscribe(workers);
        etMaster.createTable(workerTableConf, workers);

        final AtomicInteger taskIdCount = new AtomicInteger(0);

        final List<Future<TaskResult>> taskResultFutureList = new ArrayList<>(workers.size());
        workers.forEach(worker -> taskResultFutureList.add(worker.submitTask(
            Configurations.merge(TaskConfiguration.CONF
                .set(TaskConfiguration.IDENTIFIER, TASK_ID_PREFIX + taskIdCount.getAndIncrement())
                .set(TaskConfiguration.TASK, ETWorkerTask.class)
                .build(), workerConf))));

        waitAndCheckTaskResult(taskResultFutureList);

        workers.forEach(AllocatedExecutor::close);
        servers.forEach(AllocatedExecutor::close);
      });
    }
  }

  private void waitAndCheckTaskResult(final List<Future<TaskResult>> taskResultFutureList) {
    taskResultFutureList.forEach(taskResultFuture -> {
      try {
        final TaskResult taskResult = taskResultFuture.get();
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
