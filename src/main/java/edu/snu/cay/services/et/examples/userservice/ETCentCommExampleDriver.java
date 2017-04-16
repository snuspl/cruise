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
package edu.snu.cay.services.et.examples.userservice;

import edu.snu.cay.common.centcomm.master.CentCommConfProvider;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.SubmittedTask;
import edu.snu.cay.services.et.driver.impl.TaskResult;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The driver for central communication service example.
 * Launch executors which exchange messages with the driver.
 *
 * 1. Each task sends a message to the driver and waits for a response message.
 * 2. When all messages from the tasks has arrived, the driver sends response messages to the tasks.
 * 3. All tasks are terminated by the response messages.
 */
@DriverSide
@Unit
public final class ETCentCommExampleDriver {
  private static final Logger LOG = Logger.getLogger(ETCentCommExampleDriver.class.getName());

  private static final String TASK_PREFIX = "Worker-Task-";

  private final ExecutorConfiguration executorConf;

  private final ETMaster etMaster;
  private final DriverSideMsgHandler driverSideMsgHandler;

  private final int splits;

  private final AtomicInteger taskRunningCounter = new AtomicInteger(0);

  @Inject
  private ETCentCommExampleDriver(final CentCommConfProvider centCommConfProvider,
                                  final ETMaster etMaster,
                                  final DriverSideMsgHandler driverSideMsgHandler,
                                  @Parameter(Parameters.Splits.class) final int splits) {
    this.executorConf = ExecutorConfiguration.newBuilder()
        .setResourceConf(
            ResourceConfiguration.newBuilder()
                .setNumCores(1)
                .setMemSizeInMB(128)
                .build())
        .setUserContextConf(centCommConfProvider.getContextConfiguration())
        .setUserServiceConf(centCommConfProvider.getServiceConfWithoutNameResolver())
        .build();

    this.etMaster = etMaster;
    this.driverSideMsgHandler = driverSideMsgHandler;
    this.splits = splits;
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      Executors.newSingleThreadExecutor().submit(() -> {
        try {
          final List<AllocatedExecutor> executors = etMaster.addExecutors(splits, executorConf).get();

          // start update tasks on worker executors
          final AtomicInteger taskIdCount = new AtomicInteger(0);
          final List<Future<SubmittedTask>> taskFutureList = new ArrayList<>(executors.size());
          executors.forEach(executor -> taskFutureList.add(executor.submitTask(
              TaskConfiguration.CONF
                  .set(TaskConfiguration.IDENTIFIER, TASK_PREFIX + taskIdCount.getAndIncrement())
                  .set(TaskConfiguration.TASK, ETCentCommSlaveTask.class)
                  .build())));

          waitAndCheckTaskResult(taskFutureList);

          executors.forEach(AllocatedExecutor::close);
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private void waitAndCheckTaskResult(final List<Future<SubmittedTask>> taskResultFutureList) {
    taskResultFutureList.forEach(taskResultFuture -> {
      try {
        final TaskResult taskResult = taskResultFuture.get().getTaskResult();
        if (!taskResult.isSuccess()) {
          final String taskId = taskResult.getFailedTask().get().getId();
          throw new RuntimeException(String.format("Task %s has been failed", taskId));
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }

  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.INFO, "Task running: {0}", runningTask.getId());

      final int taskRunningCount = taskRunningCounter.incrementAndGet();

      if (taskRunningCount == splits) {
        driverSideMsgHandler.sendResponseAfterReceivingAllMsgs();
      }
    }
  }
}
