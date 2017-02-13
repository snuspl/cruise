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
  private static final String TASK_ID = "Simple-task";
  static final String TABLE_ID = "Table";

  private static final int NUM_EXECUTORS = 1;
  private static final ResourceConfiguration RES_CONF = ResourceConfiguration.newBuilder()
      .setNumCores(1)
      .setMemSizeInMB(128)
      .build();

  private static final Configuration TASK_CONF = TaskConfiguration.CONF
      .set(TaskConfiguration.IDENTIFIER, TASK_ID)
      .set(TaskConfiguration.TASK, SimpleETTask.class)
      .build();

  private final ETMaster etMaster;

  private final TableConfiguration tableConf;

  @Inject
  private SimpleETDriver(final ETMaster etMaster,
                         @Parameter(SimpleET.TableInputPath.class) final String tableInputPath) {
    this.etMaster = etMaster;
    this.tableConf = buildTableConf(tableInputPath);
  }

  private TableConfiguration buildTableConf(final String tableInputPath) {
    final TableConfiguration.Builder tableConfBuilder = TableConfiguration.newBuilder()
        .setId(TABLE_ID)
        .setKeyCodecClass(SerializableCodec.class)
        .setValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setPartitionFunctionClass(HashPartitionFunction.class);

    if (!tableInputPath.equals(SimpleET.TableInputPath.EMPTY)) {
      tableConfBuilder.setFilePath(tableInputPath);
    }

    return tableConfBuilder.build();
  }

  /**
   * A driver start handler for requesting executors.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final List<AllocatedExecutor> executors0 = etMaster.addExecutors(NUM_EXECUTORS, RES_CONF);
      final AllocatedTable table = etMaster.createTable(tableConf, executors0);

      final List<AllocatedExecutor> executors1 = etMaster.addExecutors(NUM_EXECUTORS, RES_CONF);
      table.subscribe(executors1);

      for (final AllocatedExecutor executor : executors0) {
        executor.submitTask(TASK_CONF);
      }

      for (final AllocatedExecutor executor : executors1) {
        executor.submitTask(TASK_CONF);
      }
    }
  }
}
