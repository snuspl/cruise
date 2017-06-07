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
package edu.snu.cay.services.et.examples.load;

import com.google.common.collect.Lists;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.SubmittedTask;
import edu.snu.cay.services.et.evaluator.api.BulkDataLoader;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.impl.DefaultDataParser;
import edu.snu.cay.services.et.evaluator.impl.ExistKeyBulkDataLoader;
import edu.snu.cay.services.et.evaluator.impl.NoneKeyBulkDataLoader;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for load example.
 * It tests whether ET can load both key, value data and none key data or not.
 * So it creates two type of {@link AllocatedTable}s which have a configuration of
 * appropriate {@link DataParser} and {@link BulkDataLoader}.
 * Then it calls {@code #table.loads()} methods to load example data into the table.
 * {@link LoadETTask} checks all data is appropriately loaded.
 */
@Unit
final class LoadETDriver {

  private static final Logger LOG = Logger.getLogger(LoadETDriver.class.getName());
  private static final int NUM_BLOCKS = 1024;
  static final String KEY_VALUE_TABLE = "Key-value-table";
  static final String NONE_KEY_TABLE = "None-key-table";
  private static final int NUM_EXECUTORS = 2;


  private final ExecutorConfiguration executorConf;
  private final ETMaster etMaster;
  private final AtomicInteger taskCounter = new AtomicInteger(0);

  private final String keyValueDataPath;
  private final String noneKeyDataPath;

  @Inject
  private LoadETDriver(final ETMaster etMaster,
                       @Parameter(LoadET.KeyValueDataPath.class) final String keyValueDataPath,
                       @Parameter(LoadET.NoneKeyDataPath.class) final String noneKeyDataPath) {
    this.etMaster = etMaster;
    this.executorConf = ExecutorConfiguration.newBuilder()
      .setResourceConf(
          ResourceConfiguration.newBuilder()
              .setNumCores(1)
              .setMemSizeInMB(128)
              .build())
      .build();
    this.keyValueDataPath = keyValueDataPath;
    this.noneKeyDataPath = noneKeyDataPath;
  }

  /**
   * Builds a configuration of table.
   * It selects the class of {@link DataParser} and {@link BulkDataLoader} by {@code #isKeyValueTable}.
   * @param tableId a identifier of table
   * @param isKeyValueTable whether a data set has key or not, it is bound to {@link IsKeyValueTable}
   */
  private TableConfiguration buildTableConf(final String tableId, final boolean isKeyValueTable) {

    final Class<? extends DataParser> dataParserClass = isKeyValueTable ?
        KVDataParser.class : DefaultDataParser.class;

    final Class<? extends BulkDataLoader> bulkDataLoaderClass = isKeyValueTable ?
        ExistKeyBulkDataLoader.class : NoneKeyBulkDataLoader.class;

    final boolean isOrderedTable = !isKeyValueTable;

    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setNumTotalBlocks(NUM_BLOCKS)
        .setKeyCodecClass(SerializableCodec.class)
        .setValueCodecClass(SerializableCodec.class)
        .setUpdateValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(isOrderedTable)
        .setDataParserClass(dataParserClass)
        .setBulkDataLoaderClass(bulkDataLoaderClass)
        .build();
  }

  /**
   * Builds a configuration of task.
   * Each task will determine what table to test by {@code #isKeyValueTable}.
   * @param isKeyValueTable whether a data set has key of not, it is bound to {@link IsKeyValueTable}
   */
  private Configuration buildTaskConf(final boolean isKeyValueTable) {
    final Configuration isKeyExistConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(IsKeyValueTable.class, String.valueOf(isKeyValueTable))
        .build();

    final Configuration taskConf = TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, "LoadETTask" + taskCounter.getAndIncrement())
        .set(TaskConfiguration.TASK, LoadETTask.class)
        .build();

    return Configurations.merge(isKeyExistConf, taskConf);
  }

  /**
   * Requests executors, loads data and submits tasks.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {

      try {

        final Future<List<AllocatedExecutor>> executorsFuture =
            etMaster.addExecutors(NUM_EXECUTORS, executorConf);

        final List<AllocatedExecutor> executors = executorsFuture.get();
        final List<Future<SubmittedTask>> taskFutureList = Lists.newArrayList();
        Executors.newSingleThreadExecutor().submit(() -> {

          try {
            final AllocatedTable keyValueTable = etMaster.createTable(
                buildTableConf(KEY_VALUE_TABLE, true), executors).get();

            final AllocatedTable noneKeyTable = etMaster.createTable(
                buildTableConf(NONE_KEY_TABLE, false), executors).get();

            long startMillis = System.currentTimeMillis();

            keyValueTable.load(executors, keyValueDataPath).get();
            LOG.log(Level.INFO, "Elapsed time of keyValueTable loading : {0}",
                System.currentTimeMillis() - startMillis);

            startMillis = System.currentTimeMillis();

            noneKeyTable.load(executors, noneKeyDataPath).get();
            LOG.log(Level.INFO, "Elapsed time of NoneKeyTable loading : {0}",
                System.currentTimeMillis() - startMillis);

            // 1. test load test that table data has both key and value.
            executors.forEach(executor -> taskFutureList.add(executor.submitTask(buildTaskConf(true))));
            for (final Future<SubmittedTask> taskFuture : taskFutureList) {
              taskFuture.get().getTaskResult();
            }
            taskFutureList.clear();

            // 2. test load test that table data has only value.
            executors.forEach(executor -> taskFutureList.add(executor.submitTask(buildTaskConf(false))));
            for (final Future<SubmittedTask> taskFuture : taskFutureList) {
              taskFuture.get().getTaskResult();
            }
            taskFutureList.clear();

          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }

          // close executors
          executors.forEach(AllocatedExecutor::close);

        });
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "unexpected exception " + e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Determine a task uses table which has keys or not.
   */
  @NamedParameter(doc = "determine a task uses table which data set has keys or not.")
  final class IsKeyValueTable implements Name<Boolean> {
  }
}
