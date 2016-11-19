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
import edu.snu.cay.services.et.driver.api.AllocatedContainer;
import edu.snu.cay.services.et.driver.api.ETMaster;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for simple example.
 */
@Unit
final class SimpleETDriver {
  private static final Logger LOG = Logger.getLogger(SimpleETDriver.class.getName());
  private static final String TASK_ID = "Simple-task";

  private static final int NUM_CONTAINERS = 1;
  private static final ResourceConfiguration RES_CONF = ResourceConfiguration.newBuilder()
      .setNumCores(2)
      .setMemSizeInMB(1024)
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
      etMaster.addContainers(NUM_CONTAINERS, RES_CONF, allocatedContainerHandler);
    }
  }

  private final EventHandler<AllocatedContainer> allocatedContainerHandler = allocatedContainer -> {
    LOG.log(Level.INFO, "Allocated container: {0}", allocatedContainer.getId());
    final Configuration taskConfiguration = TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, TASK_ID)
        .set(TaskConfiguration.TASK, SimpleETTask.class)
        .build();
    allocatedContainer.submitTask(taskConfiguration);
  };
}
