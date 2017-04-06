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
package edu.snu.cay.common.centcomm.examples;

import edu.snu.cay.common.centcomm.master.CentCommConfProvider;
import edu.snu.cay.common.param.Parameters;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The driver for CentComm service example.
 * Launch evaluators for workers which exchange CentComm messages with the driver.
 *
 * 1. Each task sends a message to the driver and waits for a response message.
 * 2. When all messages from the tasks has arrived, the driver sends response messages to the tasks.
 * 3. All tasks are terminated by the response messages.
 */
@DriverSide
@Unit
public final class CentCommExampleDriver {
  private static final Logger LOG = Logger.getLogger(CentCommExampleDriver.class.getName());

  static final String CENT_COMM_CLIENT_ID = "CENT_COMM_CLIENT_ID";

  static final String WORKER_CONTEXT_PREFIX = "Worker-Context-";
  static final String TASK_PREFIX = "Worker-Task-";

  private final CentCommConfProvider centCommConfProvider;
  private final EvaluatorRequestor evalRequestor;
  private final DriverSideMsgHandler driverSideMsgHandler;

  private final int splits;

  private final AtomicInteger evalCounter = new AtomicInteger(0);
  private final AtomicInteger taskRunningCounter = new AtomicInteger(0);

  @Inject
  private CentCommExampleDriver(final CentCommConfProvider centCommConfProvider,
                                final EvaluatorRequestor evalRequestor,
                                final DriverSideMsgHandler driverSideMsgHandler,
                                @Parameter(Parameters.Splits.class) final int splits) {
    this.centCommConfProvider = centCommConfProvider;
    this.evalRequestor = evalRequestor;
    this.driverSideMsgHandler = driverSideMsgHandler;
    this.splits = splits;
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      evalRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(splits)
          .setMemory(128)
          .setNumberOfCores(1)
          .build());
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final int workerIndex = evalCounter.getAndIncrement();
      final Configuration contextConf = Configurations.merge(
          ContextConfiguration.CONF
              .set(ContextConfiguration.IDENTIFIER, WORKER_CONTEXT_PREFIX + workerIndex)
              .build(),
          centCommConfProvider.getContextConfiguration());
      final Configuration serviceConf = centCommConfProvider.getServiceConfiguration();
      final Configuration taskConf = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, TASK_PREFIX + workerIndex)
          .set(TaskConfiguration.TASK, CentCommSlaveTask.class)
          .build();

      allocatedEvaluator.submitContextAndServiceAndTask(contextConf, serviceConf, taskConf);
    }
  }

  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.INFO, "Task running: {0}", runningTask.getId());

      final int taskRunningCount = taskRunningCounter.incrementAndGet();

      if (taskRunningCount == splits) {
        driverSideMsgHandler.validate();
      }
    }
  }
}
