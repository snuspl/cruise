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
import edu.snu.cay.services.et.evaluator.impl.HashPartitionFunction;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.List;

/**
 * Driver code for simple example.
 */
@Unit
final class SimpleETDriver {
  private static final String ASSOCIATOR_TASK_ID = "Simple-associator-task";
  private static final String SUBSCRIBER_TASK_ID = "Simple-subscriber-task";
  static final String TABLE0_ID = "Table0";
  static final String TABLE1_ID = "Table1";

  private static final ResourceConfiguration RES_CONF = ResourceConfiguration.newBuilder()
      .setNumCores(1)
      .setMemSizeInMB(128)
      .build();

  private static final Configuration ASSOCIATOR_TASK_CONF = TaskConfiguration.CONF
      .set(TaskConfiguration.IDENTIFIER, ASSOCIATOR_TASK_ID)
      .set(TaskConfiguration.TASK, AssociatorTask.class)
      .build();

  private static final Configuration SUBSCRIBER_TASK_CONF = TaskConfiguration.CONF
      .set(TaskConfiguration.IDENTIFIER, SUBSCRIBER_TASK_ID)
      .set(TaskConfiguration.TASK, SubscriberTask.class)
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
      final List<AllocatedExecutor> associators = etMaster.addExecutors(2, RES_CONF);

      final AllocatedTable table0 = etMaster.createTable(buildTableConf(TABLE0_ID, tableInputPath), associators);
      final AllocatedTable table1 =
          etMaster.createTable(buildTableConf(TABLE1_ID, SimpleET.TableInputPath.EMPTY), associators);

      final List<AllocatedExecutor> subscribers = etMaster.addExecutors(1, RES_CONF);
      table0.subscribe(subscribers);
      table1.subscribe(subscribers);

      for (final AllocatedExecutor executor : associators) {
        executor.submitTask(ASSOCIATOR_TASK_CONF);
      }

      for (final AllocatedExecutor executor : subscribers) {
        executor.submitTask(SUBSCRIBER_TASK_CONF);
      }
    }
  }
}
