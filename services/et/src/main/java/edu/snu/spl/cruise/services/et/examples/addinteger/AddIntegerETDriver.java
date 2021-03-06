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
package edu.snu.spl.cruise.services.et.examples.addinteger;

import edu.snu.spl.cruise.services.et.common.util.TaskletUtils;
import edu.snu.spl.cruise.services.et.configuration.ExecutorConfiguration;
import edu.snu.spl.cruise.services.et.configuration.ResourceConfiguration;
import edu.snu.spl.cruise.services.et.configuration.TableConfiguration;
import edu.snu.spl.cruise.services.et.configuration.TaskletConfiguration;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedExecutor;
import edu.snu.spl.cruise.services.et.driver.api.ETMaster;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedTable;
import edu.snu.spl.cruise.services.et.driver.impl.RunningTasklet;
import edu.snu.spl.cruise.services.et.examples.addinteger.parameters.*;
import edu.snu.spl.cruise.services.et.metric.MetricManager;
import edu.snu.spl.cruise.services.et.metric.configuration.MetricServiceExecutorConf;
import edu.snu.spl.cruise.utils.CatchableExecutors;
import edu.snu.spl.cruise.utils.StreamingSerializableCodec;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The AddInteger Example Driver.
 * We launch executors for workers and parameter servers upon start.
 * A model table is partitioned into server executors,
 * and worker executors update model table by running UpdaterTasks.
 * When all UpdaterTasks are complete, ValidatorTasks are run on workers.
 * When the ValidatorTask is complete, all executors are closed and
 * the Driver will shutdown because it is idle.
 */
@DriverSide
@Unit
public final class AddIntegerETDriver {
  private static final String UPDATER_TASK_ID_PREFIX = "Updater-Task-";
  private static final String VALIDATOR_TASK_ID_PREFIX = "Validator-Task-";

  static final String MODEL_TABLE_ID = "Model_Table";
  
  private final ETMaster etMaster;

  private final int numServers;
  private final int numWorkers;

  private final TableConfiguration tableConf;

  /**
   * User param configuration for {@link UpdaterTask}.
   */
  private final Configuration updaterTaskParamConf;

  /**
   * User param configuration for {@link ValidatorTask}.
   */
  private final Configuration validatorTaskParamConf;

  private final MetricServiceExecutorConf metricConf;
  private final MetricManager metricManager;

  @Inject
  private AddIntegerETDriver(final ETMaster etMaster,
                             final MetricManager metricManager,
                             @Parameter(NumServers.class) final int numServers,
                             @Parameter(NumWorkers.class) final int numWorkers,
                             @Parameter(NumUpdates.class) final int numUpdates,
                             @Parameter(StartKey.class) final int startKey,
                             @Parameter(DeltaValue.class) final int deltaValue,
                             @Parameter(UpdateCoefficient.class) final int coefficient,
                             @Parameter(NumKeys.class) final int numKeys,
                             @Parameter(MetricFlushPeriodMs.class) final long metricFlushPeriodMs) {
    this.updaterTaskParamConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(StartKey.class, Integer.toString(startKey))
        .bindNamedParameter(DeltaValue.class, Integer.toString(deltaValue))
        .bindNamedParameter(NumKeys.class, Integer.toString(numKeys))
        .bindNamedParameter(NumUpdates.class, Integer.toString(numUpdates))
        .build();

    this.validatorTaskParamConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(StartKey.class, Integer.toString(startKey))
        .bindNamedParameter(DeltaValue.class, Integer.toString(deltaValue))
        .bindNamedParameter(UpdateCoefficient.class, Integer.toString(coefficient))
        .bindNamedParameter(NumKeys.class, Integer.toString(numKeys))
        .bindNamedParameter(NumUpdates.class, Integer.toString(numUpdates))
        .bindNamedParameter(NumWorkers.class, Integer.toString(numWorkers))
        .build();

    final Configuration tableUserParamConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(UpdateCoefficient.class, Integer.toString(coefficient))
        .build();
    this.tableConf = buildTableConf(MODEL_TABLE_ID, tableUserParamConf);

    this.etMaster = etMaster;
    this.numServers = numServers;
    this.numWorkers = numWorkers;

    this.metricManager = metricManager;
    this.metricConf = MetricServiceExecutorConf.newBuilder()
        .setMetricFlushPeriodMs(metricFlushPeriodMs)
        .build();
  }

  private TableConfiguration buildTableConf(final String tableId, final Configuration userTableParamConf) {
    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(StreamingSerializableCodec.class)
        .setValueCodecClass(StreamingSerializableCodec.class)
        .setUpdateValueCodecClass(StreamingSerializableCodec.class)
        .setUpdateFunctionClass(AddIntegerUpdateFunction.class)
        .setUserParamConf(userTableParamConf)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .build();
  }

  /**
   * A driver start handler for requesting executors.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final List<AllocatedExecutor> servers;
      final List<AllocatedExecutor> workers;
      try {
        servers = etMaster.addExecutors(numServers, getExecutorConf()).get();
        workers = etMaster.addExecutors(numWorkers, getExecutorConf()).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

      CatchableExecutors.newSingleThreadExecutor().submit(() -> {
        try {

          final AllocatedTable modelTable = etMaster.createTable(tableConf, servers).get();

          // start collecting metrics from servers
          servers.forEach(server -> metricManager.startMetricCollection(server.getId(), metricConf));

          modelTable.subscribe(workers).get();

          // start update tasks on worker executors
          final AtomicInteger taskletIdCount = new AtomicInteger(0);
          final List<Future<RunningTasklet>> taskletFutureList = new ArrayList<>(workers.size());
          workers.forEach(executor -> taskletFutureList.add(executor.submitTasklet(
              TaskletConfiguration.newBuilder()
                  .setId(UPDATER_TASK_ID_PREFIX + taskletIdCount.getAndIncrement())
                  .setTaskletClass(UpdaterTask.class)
                  .setUserParamConf(updaterTaskParamConf)
                  .build())));

          TaskletUtils.waitAndCheckTaskletResult(taskletFutureList, true);

          // start validate tasks on worker executors
          taskletIdCount.set(0);
          taskletFutureList.clear();
          workers.forEach(executor -> taskletFutureList.add(executor.submitTasklet(
              TaskletConfiguration.newBuilder()
                  .setId(VALIDATOR_TASK_ID_PREFIX + taskletIdCount.getAndIncrement())
                  .setTaskletClass(ValidatorTask.class)
                  .setUserParamConf(validatorTaskParamConf)
                  .build())));

          TaskletUtils.waitAndCheckTaskletResult(taskletFutureList, true);

          workers.forEach(AllocatedExecutor::close);
          servers.forEach(AllocatedExecutor::close);
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private ExecutorConfiguration getExecutorConf() {
    return ExecutorConfiguration.newBuilder()
        .setResourceConf(ResourceConfiguration.newBuilder()
            .setNumCores(1)
            .setMemSizeInMB(128)
            .build())
        .build();
  }
}
