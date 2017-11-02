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
package edu.snu.cay.services.et.examples.checkpoint;

import edu.snu.cay.services.et.common.util.TaskUtils;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.SubmittedTask;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import edu.snu.cay.utils.CatchableExecutors;
import edu.snu.cay.utils.StreamingSerializableCodec;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for simple example.
 */
@Unit
final class CheckpointETDriver {
  private static final Logger LOG = Logger.getLogger(CheckpointETDriver.class.getName());
  private static final int NUM_TOTAL_BLOCKS = 32;
  static final int NUM_EXECUTORS = 2;
  static final String TABLE_ID = "table";

  private static final ExecutorConfiguration EXECUTOR_CONF = ExecutorConfiguration.newBuilder()
      .setResourceConf(
          ResourceConfiguration.newBuilder()
              .setNumCores(1)
              .setMemSizeInMB(128)
              .build())
      .build();

  private final ETMaster etMaster;

  @Inject
  private CheckpointETDriver(final ETMaster etMaster) {
    this.etMaster = etMaster;
  }

  private TableConfiguration buildTableConf(final String tableId) {
    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setNumTotalBlocks(NUM_TOTAL_BLOCKS)
        .setKeyCodecClass(StreamingSerializableCodec.class)
        .setValueCodecClass(StreamingSerializableCodec.class)
        .setUpdateValueCodecClass(StreamingSerializableCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
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
      final List<AllocatedExecutor> executors0;
      try {
        executors0 = etMaster.addExecutors(NUM_EXECUTORS, EXECUTOR_CONF).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

      CatchableExecutors.newSingleThreadExecutor().submit(() -> {
        try {
          // 1. Checkpoint a table after putting some value by running PutTask.
          final AllocatedTable originalTable = etMaster.createTable(buildTableConf(TABLE_ID), executors0).get();

          final Future<SubmittedTask> taskFuture = executors0.get(0).submitTask(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, "PutTask")
              .set(TaskConfiguration.TASK, PutTask.class)
              .build());

          TaskUtils.waitAndCheckTaskResult(Collections.singletonList(taskFuture), true);

          final String chkpId = originalTable.checkpoint().get();
          LOG.log(Level.INFO, "checkpointId: {0}", chkpId);

          originalTable.drop().get();

          // 2. Restore a table from the checkpoint into another set of executors.
          // Then check that table contents are correctly restored by running GetTask.
          final List<AllocatedExecutor> executors1;
          try {
            executors1 = etMaster.addExecutors(NUM_EXECUTORS, EXECUTOR_CONF).get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }

          final AllocatedTable tableFromLocalChkps = etMaster.createTable(chkpId, executors1).get();

          final AtomicInteger taskIdx = new AtomicInteger(0);
          final List<Future<SubmittedTask>> taskFutureList = new ArrayList<>(NUM_EXECUTORS);
          executors1.forEach(executor -> taskFutureList.add(executor.submitTask(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, "GetTask" + taskIdx.getAndIncrement())
              .set(TaskConfiguration.TASK, GetTask.class)
              .build())));

          TaskUtils.waitAndCheckTaskResult(taskFutureList, true);
          taskFutureList.clear();

          tableFromLocalChkps.drop().get();

          // 3. Close executors0, which have chkps temporally in their local file system.
          // The executors will commit chkps on their close.
          final List<Future> closeFutureList = new ArrayList<>(NUM_EXECUTORS);
          executors0.forEach(executor -> closeFutureList.add(executor.close()));
          closeFutureList.forEach(future -> {
            try {
              future.get();
            } catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e);
            }
          });

          // 4. Restore a table once again to check that chkps are committed safely.
          final AllocatedTable tableFromCommittedChkps = etMaster.createTable(chkpId, executors1).get();

          executors1.forEach(executor -> taskFutureList.add(executor.submitTask(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, "GetTask" + taskIdx.getAndIncrement())
              .set(TaskConfiguration.TASK, GetTask.class)
              .build())));

          TaskUtils.waitAndCheckTaskResult(taskFutureList, true);
          taskFutureList.clear();

          // checkpoint the table with sampling rate 0.5
          final double samplingRatio = 0.5;
          final String sampledChkpId = tableFromCommittedChkps.checkpoint(samplingRatio).get();

          tableFromCommittedChkps.drop().get();

          // 5. Restore a table from the checkpoint with sampling rate 0.5
          final AllocatedTable tableFromSampledChkp = etMaster.createTable(sampledChkpId, executors1).get();

          executors1.forEach(executor -> taskFutureList.add(executor.submitTask(
              Configurations.merge(
                  TaskConfiguration.CONF
                      .set(TaskConfiguration.IDENTIFIER, "GetTask" + taskIdx.getAndIncrement())
                      .set(TaskConfiguration.TASK, GetTask.class)
                      .build(),
                  Tang.Factory.getTang().newConfigurationBuilder()
                      .bindNamedParameter(SamplingRatio.class, Double.toString(samplingRatio))
                      .build())
          )));

          TaskUtils.waitAndCheckTaskResult(taskFutureList, true);
          taskFutureList.clear();

          tableFromSampledChkp.drop().get();

          executors1.forEach(AllocatedExecutor::close);

        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  @NamedParameter(doc = "A sampling rate for a table checkpoint", default_value = "1.0")
  final class SamplingRatio implements Name<Double> {
  }
}
