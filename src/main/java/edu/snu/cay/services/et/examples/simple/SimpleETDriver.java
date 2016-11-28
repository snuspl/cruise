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
import edu.snu.cay.services.et.driver.api.AllocatedContainer;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.MaterializedTable;
import edu.snu.cay.services.et.evaluator.impl.HashPartitionFunction;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import edu.snu.cay.services.et.exceptions.NotAssociatedTableException;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
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

  private static final int NUM_CONTAINERS = 1;
  private static final ResourceConfiguration RES_CONF = ResourceConfiguration.newBuilder()
      .setNumCores(1)
      .setMemSizeInMB(128)
      .build();

  private static final TableConfiguration TABLE_CONF = TableConfiguration.newBuilder()
      .setId("table")
      .setKeyCodecClass(SerializableCodec.class)
      .setValueCodecClass(SerializableCodec.class)
      .setUpdateFunctionClass(VoidUpdateFunction.class)
      .setPartitionFunctionClass(HashPartitionFunction.class)
      .build();

  private static final Configuration TASK_CONF = TaskConfiguration.CONF
      .set(TaskConfiguration.IDENTIFIER, TASK_ID)
      .set(TaskConfiguration.TASK, SimpleETTask.class)
      .build();

  private final ETMaster etMaster;

  @Inject
  private SimpleETDriver(final ETMaster etMaster) {
    this.etMaster = etMaster;
  }

  /**
   * A driver start handler for requesting containers.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final List<AllocatedContainer> containers0 = etMaster.addContainers(NUM_CONTAINERS, RES_CONF);
      final MaterializedTable table;
      try {
        table = etMaster.createTable(TABLE_CONF).associate(containers0).materialize();
      } catch (final NotAssociatedTableException e) {
        throw new RuntimeException("Table is not associated. It's not ready to be materialized.", e);
      }

      final List<AllocatedContainer> containers1 = etMaster.addContainers(NUM_CONTAINERS, RES_CONF);
      table.subscribe(containers0).subscribe(containers1);

      for (final AllocatedContainer container : containers0) {
        container.submitTask(TASK_CONF);
      }

      for (final AllocatedContainer container : containers1) {
        container.submitTask(TASK_CONF);
      }
    }
  }
}
