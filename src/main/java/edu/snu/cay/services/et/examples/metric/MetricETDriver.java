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
package edu.snu.cay.services.et.examples.metric;

import edu.snu.cay.services.et.common.util.TaskUtils;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.SubmittedTask;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import edu.snu.cay.services.et.metric.MetricManager;
import edu.snu.cay.services.et.metric.configuration.MetricServiceExecutorConf;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Driver code for simple example.
 */
@Unit
final class MetricETDriver {
  private static final JavaConfigurationBuilder EMPTY_CONF_BUILDER = Tang.Factory.getTang().newConfigurationBuilder();

  private static final String METRIC_TASK_ID_PREFIX = "Metric-task-";
  static final int NUM_ASSOCIATORS = 2; // should be at least 2
  private static final String TABLE_ID = "Dummy-table";

  private final long metricAutomaticFlushPeriodMs;
  private final long metricManualFlushPeriodMs;
  private final long customMetricRecordPeriodMs;
  private final long taskDurationMs;

  private ExecutorConfiguration getExecutorConf() {
    return ExecutorConfiguration.newBuilder()
        .setResourceConf(
            ResourceConfiguration.newBuilder()
                .setNumCores(1)
                .setMemSizeInMB(128)
                .build())
        .setUserContextConf(
            EMPTY_CONF_BUILDER
                .bindNamedParameter(MetricET.TaskDurationMs.class, Long.toString(taskDurationMs))
                .bindNamedParameter(MetricET.CustomMetricRecordPeriodMs.class,
                    Long.toString(customMetricRecordPeriodMs))
                .bindNamedParameter(MetricET.MetricManualFlushPeriodMs.class, Long.toString(metricManualFlushPeriodMs))
                .build()
        )
        .build();
  }

  private final ETMaster etMaster;
  private final MetricManager metricManager;

  @Inject
  private MetricETDriver(final ETMaster etMaster,
                         final MetricManager metricManager,
                         @Parameter(MetricET.MetricManualFlushPeriodMs.class) final long metricManualFlushPeriodMs,
                         @Parameter(MetricET.MetricAutomaticFlushPeriodMs.class)
                         final long metricAutomaticFlushPeriodMs,
                         @Parameter(MetricET.CustomMetricRecordPeriodMs.class) final long customMetricRecordPeriodMs,
                         @Parameter(MetricET.TaskDurationMs.class) final long taskDurationMs) {
    this.etMaster = etMaster;
    this.metricManager = metricManager;
    this.metricAutomaticFlushPeriodMs = metricAutomaticFlushPeriodMs;
    this.metricManualFlushPeriodMs = metricManualFlushPeriodMs;
    this.customMetricRecordPeriodMs = customMetricRecordPeriodMs;
    this.taskDurationMs = taskDurationMs;
  }

  private TableConfiguration buildTableConf(final String tableId) {
    final TableConfiguration.Builder tableConfBuilder = TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(SerializableCodec.class)
        .setValueCodecClass(SerializableCodec.class)
        .setUpdateValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(false);

    return tableConfBuilder.build();
  }

  /**
   * A driver start handler for requesting executors.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final List<AllocatedExecutor> associators;
      try {
        associators = etMaster.addExecutors(NUM_ASSOCIATORS, getExecutorConf()).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

      Executors.newSingleThreadExecutor().submit(() -> {
        try {
          final AtomicInteger taskIdCount = new AtomicInteger(0);
          final List<Future<SubmittedTask>> taskFutureList = new ArrayList<>(associators.size());

          // Simply create a hash-based table.
          etMaster.createTable(buildTableConf(TABLE_ID), associators).get();

          // Round 1. start collecting metrics only by manual flush
          associators.forEach(associator -> metricManager.startMetricCollection(associator.getId(),
              MetricServiceExecutorConf.newBuilder()
                  .build()));

          // Run tasks
          associators.forEach(executor -> taskFutureList.add(executor.submitTask(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, METRIC_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
              .set(TaskConfiguration.TASK, MetricTask.class)
              .build())));

          TaskUtils.waitAndCheckTaskResult(taskFutureList, true);

          // Round 2. start collecting metrics with automatic periodic flush
          associators.forEach(associator -> metricManager.startMetricCollection(associator.getId(),
              MetricServiceExecutorConf.newBuilder()
                  .setMetricFlushPeriodMs(metricAutomaticFlushPeriodMs)
                  .build()));

          // Run tasks
          associators.forEach(executor -> taskFutureList.add(executor.submitTask(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, METRIC_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
              .set(TaskConfiguration.TASK, MetricTask.class)
              .build())));

          TaskUtils.waitAndCheckTaskResult(taskFutureList, true);

          // Close the executors
          associators.forEach(AllocatedExecutor::close);

        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }
}
