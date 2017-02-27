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

import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.TaskResult;
import edu.snu.cay.services.et.driver.impl.MigrationResult;
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
  static final int NUM_ASSOCIATORS = 2; // should be at least 2
  static final int NUM_SUBSCRIBERS = 1; // should be at least 1
  static final String HASHED_TABLE_ID = "Hashed_Table";
  static final String ORDERED_TABLE_ID = "Ordered_Table";
  static final String ORDERED_TABLE_WITH_FILE_ID = "Ordered_Table_WITH_FILE";

  private static final ResourceConfiguration RES_CONF = ResourceConfiguration.newBuilder()
      .setNumCores(1)
      .setMemSizeInMB(128)
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
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setIsOrderedTable(isOrderedTable);

    if (!inputPath.equals(SimpleET.TableInputPath.EMPTY)) {
      tableConfBuilder.setFilePath(inputPath);
    }

    return tableConfBuilder.build();
  }

  /**
   * A driver start handler for requesting executors.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final List<AllocatedExecutor> associators = etMaster.addExecutors(NUM_ASSOCIATORS, RES_CONF);

      final AllocatedTable hashedTable = etMaster.createTable(buildTableConf(HASHED_TABLE_ID,
          SimpleET.TableInputPath.EMPTY, false), associators);
      final AllocatedTable orderedTable = etMaster.createTable(buildTableConf(ORDERED_TABLE_ID,
          SimpleET.TableInputPath.EMPTY, true), associators);

      final List<AllocatedExecutor> subscribers = etMaster.addExecutors(NUM_SUBSCRIBERS, RES_CONF);
      hashedTable.subscribe(subscribers);
      orderedTable.subscribe(subscribers);

      final AtomicInteger taskIdCount = new AtomicInteger(0);

      // 1. First start a put task in a subscriber
      final Future<TaskResult> putTaskResultFuture = subscribers.get(0).submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, PUT_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
          .set(TaskConfiguration.TASK, PutTask.class)
          .build());

      // 2. wait until a put task finished
      try {
        assert putTaskResultFuture.get().isSuccess();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

      // 3. Then start get tasks in all executors
      associators.forEach(executor ->
          executor.submitTask(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, GET_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
              .set(TaskConfiguration.TASK, GetTask.class)
              .build()));

      subscribers.forEach(executor ->
          executor.submitTask(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, GET_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
              .set(TaskConfiguration.TASK, GetTask.class)
              .build()));

      // 4. migrate blocks between associators
      final CountDownLatch migrationLatch = new CountDownLatch(2);
      final EventHandler<MigrationResult> migrationCallback = migrationResult -> {
        LOG.log(Level.INFO, "Migration has been finished: {0}, {1}, {2}",
            new Object[]{migrationResult.isCompleted(), migrationResult.getMsg(), migrationResult.getMigratedBlocks()});
        migrationLatch.countDown();
      };

      // move all blocks of hashedTable in the first associator to the second associator
      hashedTable.moveBlocks(associators.get(0).getId(), associators.get(1).getId(),
          Integer.parseInt(NumTotalBlocks.DEFAULT_VALUE_STR), migrationCallback);

      // move all blocks of orderedTable in the second associator to the first associator
      orderedTable.moveBlocks(associators.get(1).getId(), associators.get(0).getId(),
          Integer.parseInt(NumTotalBlocks.DEFAULT_VALUE_STR), migrationCallback);

      // wait until migrations finish
      try {
        migrationLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }

      // 5. start get tasks in all executors again after migration
      final List<Future<TaskResult>> taskResultFutureList = new ArrayList<>(associators.size() + subscribers.size());
      associators.forEach(executor -> taskResultFutureList.add(executor.submitTask(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, GET_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
            .set(TaskConfiguration.TASK, GetTask.class)
            .build())));

      subscribers.forEach(executor -> taskResultFutureList.add(executor.submitTask(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, GET_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
              .set(TaskConfiguration.TASK, GetTask.class)
              .build())));

      // 6. wait all tasks finished and close executors
      taskResultFutureList.forEach(taskResultFuture -> {
        try {
          assert taskResultFuture.get().isSuccess();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });

      // 7. create a table with input file
      final AllocatedTable orderedTableWithFile = etMaster.createTable(buildTableConf(ORDERED_TABLE_WITH_FILE_ID,
          tableInputPath, true), associators);

      // 8. start tasks to confirm that the last table has been loaded correctly
      // TODO #27: Provide a way to access data in local blocks

      subscribers.forEach(AllocatedExecutor::close);
      associators.forEach(AllocatedExecutor::close);
    }
  }
}
