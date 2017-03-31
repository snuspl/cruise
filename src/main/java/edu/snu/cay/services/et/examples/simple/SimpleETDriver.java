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
package edu.snu.cay.services.et.examples.simple;

import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.TaskResult;
import edu.snu.cay.services.et.driver.impl.MigrationResult;
import edu.snu.cay.services.et.evaluator.impl.DefaultDataParser;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.serialization.SerializableCodec;
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
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for simple example.
 */
@Unit
final class SimpleETDriver {
  private static final Logger LOG = Logger.getLogger(SimpleETDriver.class.getName());
  private static final String GET_TASK_ID_PREFIX = "Simple-get-task-";
  private static final String PUT_TASK_ID_PREFIX = "Simple-put-task-";
  private static final String SCAN_TASK_ID_PREFIX = "Simple-scan-task-";
  static final int NUM_ASSOCIATORS = 2; // should be at least 2
  static final int NUM_SUBSCRIBERS = 1; // should be at least 1
  static final String HASHED_TABLE_ID = "Hashed_Table";
  static final String ORDERED_TABLE_ID = "Ordered_Table";
  static final String ORDERED_TABLE_WITH_FILE_ID = "Ordered_Table_With_File";

  private static final ExecutorConfiguration EXECUTOR_CONF = ExecutorConfiguration.newBuilder()
      .setResourceConf(
          ResourceConfiguration.newBuilder()
              .setNumCores(1)
              .setMemSizeInMB(128)
              .build())
      .build();

  private final ETMaster etMaster;

  private final String tableInputPath;

  @Inject
  private SimpleETDriver(final ETMaster etMaster,
                         @Parameter(SimpleET.TableInputPath.class) final String tableInputPath) {
    this.etMaster = etMaster;
    this.tableInputPath = tableInputPath;
  }

  private TableConfiguration buildTableConf(final String tableId, final String inputPath,
                                            final boolean isOrderedTable) {
    final TableConfiguration.Builder tableConfBuilder = TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(SerializableCodec.class)
        .setValueCodecClass(SerializableCodec.class)
        .setUpdateValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setIsOrderedTable(isOrderedTable);

    if (!inputPath.equals(SimpleET.TableInputPath.EMPTY)) {
      tableConfBuilder.setFilePath(inputPath);
      tableConfBuilder.setDataParserClass(DefaultDataParser.class);
    }

    return tableConfBuilder.build();
  }

  /**
   * A driver start handler for requesting executors.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final List<AllocatedExecutor> associators = etMaster.addExecutors(NUM_ASSOCIATORS, EXECUTOR_CONF);

      final AllocatedTable hashedTable = etMaster.createTable(buildTableConf(HASHED_TABLE_ID,
          SimpleET.TableInputPath.EMPTY, false), associators);
      final AllocatedTable orderedTable = etMaster.createTable(buildTableConf(ORDERED_TABLE_ID,
          SimpleET.TableInputPath.EMPTY, true), associators);

      final List<AllocatedExecutor> subscribers = etMaster.addExecutors(NUM_SUBSCRIBERS, EXECUTOR_CONF);
      hashedTable.subscribe(subscribers);
      orderedTable.subscribe(subscribers);

      final AtomicInteger taskIdCount = new AtomicInteger(0);
      final List<Future<TaskResult>> taskResultFutureList = new ArrayList<>(associators.size() + subscribers.size());

      // 1. First run a put task in a subscriber
      taskResultFutureList.add(subscribers.get(0).submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, PUT_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
          .set(TaskConfiguration.TASK, PutTask.class)
          .build()));

      waitAndCheckTaskResult(taskResultFutureList);

      // 2. Then run get tasks in all executors
      taskResultFutureList.clear();

      associators.forEach(executor -> taskResultFutureList.add(executor.submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, GET_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
          .set(TaskConfiguration.TASK, GetTask.class)
          .build())));

      subscribers.forEach(executor -> taskResultFutureList.add(executor.submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, GET_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
          .set(TaskConfiguration.TASK, GetTask.class)
          .build())));

      waitAndCheckTaskResult(taskResultFutureList);

      // 3. migrate blocks between associators
      final CountDownLatch migrationLatch1 = new CountDownLatch(2);
      final EventHandler<MigrationResult> migrationCallback1 = migrationResult -> {
        LOG.log(Level.INFO, "Migration has been finished: {0}, {1}, {2}",
            new Object[]{migrationResult.isCompleted(), migrationResult.getMsg(), migrationResult.getMigratedBlocks()});
        migrationLatch1.countDown();
      };

      // move all blocks of hashedTable in the first associator to the second associator
      hashedTable.moveBlocks(associators.get(0).getId(), associators.get(1).getId(),
          Integer.parseInt(NumTotalBlocks.DEFAULT_VALUE_STR), migrationCallback1);

      // move all blocks of orderedTable in the second associator to the first associator
      orderedTable.moveBlocks(associators.get(1).getId(), associators.get(0).getId(),
          Integer.parseInt(NumTotalBlocks.DEFAULT_VALUE_STR), migrationCallback1);

      // wait until migrations finish
      try {
        migrationLatch1.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }

      // 4. run get tasks in all executors again after migration
      taskResultFutureList.clear();

      associators.forEach(executor -> taskResultFutureList.add(executor.submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, GET_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
          .set(TaskConfiguration.TASK, GetTask.class)
          .build())));

      subscribers.forEach(executor -> taskResultFutureList.add(executor.submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, GET_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
          .set(TaskConfiguration.TASK, GetTask.class)
          .build())));

      waitAndCheckTaskResult(taskResultFutureList);

      // 5. create a table with input file
      final AllocatedTable orderedTableWithFile = etMaster.createTable(buildTableConf(ORDERED_TABLE_WITH_FILE_ID,
          tableInputPath, true), associators);

      // 6. start scan tasks in associator executors
      taskResultFutureList.clear();

      associators.forEach(executor -> taskResultFutureList.add(executor.submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, SCAN_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
          .set(TaskConfiguration.TASK, ScanTask.class)
          .build())));

      waitAndCheckTaskResult(taskResultFutureList);

      // 7. migrate blocks between associators
      final CountDownLatch migrationLatch2 = new CountDownLatch(1);
      final EventHandler<MigrationResult> migrationCallback2 = migrationResult -> {
        LOG.log(Level.INFO, "Migration has been finished: {0}, {1}, {2}",
            new Object[]{migrationResult.isCompleted(), migrationResult.getMsg(), migrationResult.getMigratedBlocks()});
        migrationLatch2.countDown();
      };

      // move all blocks of orderedTableWithFile in the second associator to the first associator
      orderedTableWithFile.moveBlocks(associators.get(1).getId(), associators.get(0).getId(),
          Integer.parseInt(NumTotalBlocks.DEFAULT_VALUE_STR), migrationCallback2);

      // wait until migration finish
      try {
        migrationLatch2.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }

      // 8. start scan tasks again after migration
      taskResultFutureList.clear();

      associators.forEach(executor -> taskResultFutureList.add(executor.submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, SCAN_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
          .set(TaskConfiguration.TASK, ScanTask.class)
          .build())));

      waitAndCheckTaskResult(taskResultFutureList);

      // 9. close executors
      subscribers.forEach(AllocatedExecutor::close);
      associators.forEach(AllocatedExecutor::close);
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
