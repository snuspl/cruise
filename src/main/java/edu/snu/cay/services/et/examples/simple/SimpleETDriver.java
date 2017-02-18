/*
 * Copyright (C) 2016 Seoul National University
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
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.TaskResult;
import edu.snu.cay.services.et.evaluator.impl.HashPartitionFunction;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Driver code for simple example.
 */
@Unit
final class SimpleETDriver {
  private static final String GET_TASK_ID_PREFIX = "Simple-get-task-";
  private static final String PUT_TASK_ID_PREFIX = "Simple-put-task-";
  static final int NUM_ASSOCIATORS = 2;
  static final int NUM_SUBSCRIBERS = 2;
  static final String TABLE0_ID = "Table0";
  static final String TABLE1_ID = "Table1";

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

  private TableConfiguration buildTableConf(final String tableId, final String inputPath) {
    final TableConfiguration.Builder tableConfBuilder = TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(SerializableCodec.class)
        .setValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setPartitionFunctionClass(HashPartitionFunction.class);

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

      final AllocatedTable table0 = etMaster.createTable(buildTableConf(TABLE0_ID, tableInputPath), associators);
      final AllocatedTable table1 =
          etMaster.createTable(buildTableConf(TABLE1_ID, SimpleET.TableInputPath.EMPTY), associators);

      final List<AllocatedExecutor> subscribers = etMaster.addExecutors(NUM_SUBSCRIBERS, RES_CONF);
      table0.subscribe(subscribers);
      table1.subscribe(subscribers);

      final AtomicInteger taskIdCount = new AtomicInteger(0);

      // 1. First start a put task in a subscriber
      final Future<TaskResult> taskResultFuture = subscribers.get(0).submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, PUT_TASK_ID_PREFIX + taskIdCount.getAndIncrement())
          .set(TaskConfiguration.TASK, PutTask.class)
          .build());

      // 2. wait until a put task finished
      try {
        taskResultFuture.get();
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

      subscribers.forEach(AllocatedExecutor::close);
      associators.forEach(AllocatedExecutor::close);
    }
  }
}
