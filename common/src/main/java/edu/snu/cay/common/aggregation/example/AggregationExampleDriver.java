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
package edu.snu.cay.common.aggregation.example;

import edu.snu.cay.common.aggregation.AggregationDriver;
import edu.snu.cay.common.param.Parameters;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.CompletedTask;
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
 * The driver for aggregation service example.
 * Launch evaluators for workers, which send aggregation message to master(driver).
 * When all tasks are complete, checks that all messages are arrived or not.
 */
@DriverSide
@Unit
public final class AggregationExampleDriver {
  private static final Logger LOG = Logger.getLogger(AggregationExampleDriver.class.getName());

  private static final String WORKER_CONTEXT_PREFIX = "Worker-Context-";
  private static final String TASK_PREFIX = "Worker-Task-";

  private final AggregationDriver aggregationDriver;
  private final EvaluatorRequestor evalRequestor;
  private final DriverSideMsgHandler driverSideMsgHandler;

  private final int splits;

  private final AtomicInteger evalCounter = new AtomicInteger(0);
  private final AtomicInteger taskCompletedCounter = new AtomicInteger(0);

  @Inject
  private AggregationExampleDriver(final AggregationDriver aggregationDriver,
                                   final EvaluatorRequestor evalRequestor,
                                   final DriverSideMsgHandler driverSideMsgHandler,
                                   @Parameter(Parameters.Splits.class) final int splits) {
    this.aggregationDriver = aggregationDriver;
    this.evalRequestor = evalRequestor;
    this.driverSideMsgHandler = driverSideMsgHandler;
    this.splits = splits;
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      evalRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(splits)
          .setMemory(256)
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
          aggregationDriver.getContextConfiguration());
      final Configuration serviceConf = aggregationDriver.getServiceConfiguration();
      final Configuration taskConf = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, TASK_PREFIX + workerIndex)
          .set(TaskConfiguration.TASK, AggregationSlaveTask.class)
          .build();

      allocatedEvaluator.submitContextAndServiceAndTask(contextConf, serviceConf, taskConf);
    }
  }

  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.INFO, "Task running: {0}", runningTask.getId());
    }
  }

  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {
      LOG.log(Level.INFO, "Task completed: {0}", completedTask.getId());

      final int taskCompletedCount = taskCompletedCounter.incrementAndGet();

      if (taskCompletedCount == evalCounter.get()) {
        driverSideMsgHandler.validate();
      }
      completedTask.getActiveContext().close();
    }
  }
}
