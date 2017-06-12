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
package edu.snu.cay.services.et.examples.tableaccess;

import edu.snu.cay.common.centcomm.master.CentCommConfProvider;
import edu.snu.cay.services.et.common.util.TaskUtils;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.RemoteAccessConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.SubmittedTask;
import edu.snu.cay.services.et.examples.tableaccess.parameters.BlockAccessType;
import edu.snu.cay.services.et.examples.tableaccess.parameters.KeyOffsetByExecutor;
import edu.snu.cay.services.et.examples.tableaccess.parameters.NumExecutorsToRunTask;
import edu.snu.cay.services.et.examples.tableaccess.parameters.TableIdentifier;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Driver code for table access example.
 *
 * It submits a test task to executors upon the following criteria.
 * - Where executors the task runs in : associator vs subscriber.
 * - Which table the task uses : hashed based table vs ordering based table
 * - Which access pattern the task has : Random vs All blocks vs One blocks
 */
@Unit
final class TableAccessETDriver {
  static final String CENTCOMM_CLIENT_ID = "CENTCOMM_CLIENT_ID";
  static final int NUM_EXECUTORS = 3; // the number of executors for each associators and subscribers
  static final int NUM_BLOCKS = 256;

  // access patterns
  static final String RANDOM_ACCESS = "RAND"; // random-access
  static final String ONE_BLOCK_ACCESS = "ONE"; // one-block-access
  static final String ALL_BLOCKS_ACCESS = "ALL"; // all-blocks-access

  private final ExecutorConfiguration executorConf;
  private final ETMaster etMaster;
  private final JobMessageObserver jobMessageObserver;

  @Inject
  private TableAccessETDriver(final ETMaster etMaster,
                              final CentCommConfProvider centCommConfProvider,
                              final JobMessageObserver jobMessageObserver) {
    this.etMaster = etMaster;
    this.executorConf = ExecutorConfiguration.newBuilder()
        .setResourceConf(ResourceConfiguration.newBuilder()
            .setNumCores(1)
            .setMemSizeInMB(128)
            .build())
        .setRemoteAccessConf(RemoteAccessConfiguration.newBuilder()
            .setHandlerQueueSize(2048)
            .setNumHandlerThreads(1)
            .setSenderQueueSize(2048)
            .setNumSenderThreads(1)
            .build())
        .setUserContextConf(centCommConfProvider.getContextConfiguration())
        .setUserServiceConf(centCommConfProvider.getServiceConfWithoutNameResolver())
        .build();
    this.jobMessageObserver = jobMessageObserver;
  }

  private TableConfiguration buildTableConf(final String tableId,
                                            final boolean isOrderedTable) {
    final TableConfiguration.Builder tableConfBuilder = TableConfiguration.newBuilder()
        .setId(tableId)
        .setNumTotalBlocks(NUM_BLOCKS)
        .setKeyCodecClass(SerializableCodec.class)
        .setValueCodecClass(SerializableCodec.class)
        .setUpdateValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(PrefixUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(isOrderedTable);

    return tableConfBuilder.build();
  }

  /**
   * A driver start handler for requesting executors.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {

      try {
        final Future<List<AllocatedExecutor>> associatorsFuture = etMaster.addExecutors(NUM_EXECUTORS, executorConf);
        final Future<List<AllocatedExecutor>> subscribersFuture = etMaster.addExecutors(NUM_EXECUTORS, executorConf);

        final List<AllocatedExecutor> associators = associatorsFuture.get();
        final List<AllocatedExecutor> subscribers = subscribersFuture.get();

        Executors.newSingleThreadExecutor().submit(() -> {

          // Single thread test.
          // 1. Run TableAccess ET tasks with random block access type.
          // case 1-1. subscribers access an ordering based table with random access pattern
          runTest(RANDOM_ACCESS, true, associators, subscribers, false);
          // case 1-2. subscribers access an hash based table with random access pattern
          runTest(RANDOM_ACCESS, false, associators, subscribers, false);
          // case 1-3. associators access an ordering based table with random access pattern
          runTest(RANDOM_ACCESS, true, associators, subscribers, true);
          // case 1-4. associators access an hash based table with random access pattern
          runTest(RANDOM_ACCESS, false, associators, subscribers, true);

          // 2. Run TableAccess ET tasks with one block and all blocks access type. (applicable only to ordered table).
          // case 2-1. subscribers access only one block of an ordering based table
          runTest(ONE_BLOCK_ACCESS, true, associators, subscribers, false);
          // case 2-2. associators access only one block of an ordering based table
          runTest(ONE_BLOCK_ACCESS, true, associators, subscribers, true);
          // case 2-3. subscribers access all blocks of an ordering based table
          runTest(ALL_BLOCKS_ACCESS, true, associators, subscribers, false);
          // case 2-4. associators access all blocks of an ordering based table
          runTest(ALL_BLOCKS_ACCESS, true, associators, subscribers, true);

          // close executors
          subscribers.forEach(AllocatedExecutor::close);
          associators.forEach(AllocatedExecutor::close);

        });
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Runs a single-thread table access test.
   * At first, it creates a table that tasks will use.
   * Then for each test executor, it submits a task with corresponding parameter configuration.
   * @param tableAccessType the type of table access pattern
   * @param isOrderedTable whether the table is ordering-based or not.
   * @param associators associators of a table
   * @param subscribers subscribers of a table
   * @param taskOnAssociators whether to submit tasks to associators or subscribers
   */
  private void runTest(final String tableAccessType,
                       final boolean isOrderedTable,
                       final List<AllocatedExecutor> associators,
                       final List<AllocatedExecutor> subscribers,
                       final boolean taskOnAssociators) {
    try {
      // build test Id based on the test type
      // For example: ASSOCIATORS access ORDERING-based tables with RANDOM access pattern  ASSO_ORDE_RAND
      final String testId =
          (taskOnAssociators ? "ASSO_" : "SUBS_") + (isOrderedTable ? "ORDE_" : "HASH_") + isOrderedTable;

      sendMessageToClient("Start a table access test. TestId: " + testId);

      final String tableId = testId; // use table ID by test ID

      final List<AllocatedExecutor> executorsToSubmitTask = taskOnAssociators ? associators : subscribers;

      // create a table to use
      final AllocatedTable table = etMaster.createTable(buildTableConf(tableId, isOrderedTable), associators).get();
      table.subscribe(subscribers).get();

      // launch tasks to executors
      final List<Future<SubmittedTask>> taskFutureList = new ArrayList<>(executorsToSubmitTask.size());
      int taskIdx = 0;
      for (final AllocatedExecutor testExecutor : executorsToSubmitTask) {
        final Configuration taskParamsConf = getTaskParamsConf(taskIdx, tableId,
            tableAccessType, executorsToSubmitTask.size());
        final Configuration taskConf = getTaskConf(testId, taskIdx);
        taskFutureList.add(testExecutor.submitTask(Configurations.merge(taskParamsConf, taskConf)));
        taskIdx++;
      }

      // wait and check the result
      TaskUtils.waitAndCheckTaskResult(taskFutureList, true);
      table.drop().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private static Configuration getTaskParamsConf(final int taskIdx,
                                                 final String tableId,
                                                 final String blockAccessType,
                                                 final int testExecutorsSize) {

    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(KeyOffsetByExecutor.class, Integer.toString(taskIdx))
        .bindNamedParameter(TableIdentifier.class, tableId)
        .bindNamedParameter(BlockAccessType.class, blockAccessType)
        .bindNamedParameter(NumExecutorsToRunTask.class, Integer.toString(testExecutorsSize))
        .build();
  }

  private static Configuration getTaskConf(final String testId,
                                           final int taskIdx) {
    return TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, testId + "-" + taskIdx)
        .set(TaskConfiguration.TASK, TableAccessSingleThreadTask.class)
        .build();
  }

  private void sendMessageToClient(final String message) {
    jobMessageObserver.sendMessageToClient(message.getBytes());
  }
}
