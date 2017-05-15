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
package edu.snu.cay.pregel;

import edu.snu.cay.pregel.graph.parameters.NumPartitions;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for Pregel applications.
 */
@Unit
public final class PregelMaster {

  private static final Logger LOG = Logger.getLogger(PregelMaster.class.getName());

  /**
   * Total number of partitions for each workers.
   */
  private static final int NUM_PARTITIONS = 4;

  private final EvaluatorRequestor evaluatorRequestor;

  @Inject
  private PregelMaster(final EvaluatorRequestor evaluatorRequestor) {
    this.evaluatorRequestor = evaluatorRequestor;
  }

  public final class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {
      evaluatorRequestor.newRequest()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(1)
          .submit();

      LOG.log(Level.INFO, "Requested Evaluator");
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting Pregel task to AllocatedEvaluator: {0}", allocatedEvaluator);
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "PregelWorkerTask")
          .set(TaskConfiguration.TASK, PregelWorkerTask.class)
          .build();

      final Configuration partitionNumberConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(NumPartitions.class, String.valueOf(NUM_PARTITIONS))
          .build();
      allocatedEvaluator.submitTask(Configurations.merge(taskConfiguration, partitionNumberConf));


      LOG.log(Level.INFO, "Submitted Pregel task");
    }
  }
}
