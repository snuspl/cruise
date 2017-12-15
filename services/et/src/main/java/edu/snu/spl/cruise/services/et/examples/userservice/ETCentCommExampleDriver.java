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
package edu.snu.spl.cruise.services.et.examples.userservice;

import edu.snu.spl.cruise.common.centcomm.master.CentCommConfProvider;
import edu.snu.spl.cruise.common.param.Parameters;
import edu.snu.spl.cruise.services.et.common.util.TaskletUtils;
import edu.snu.spl.cruise.services.et.configuration.ExecutorConfiguration;
import edu.snu.spl.cruise.services.et.configuration.ResourceConfiguration;
import edu.snu.spl.cruise.services.et.configuration.TaskletConfiguration;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedExecutor;
import edu.snu.spl.cruise.services.et.driver.api.ETMaster;
import edu.snu.spl.cruise.services.et.driver.impl.RunningTasklet;
import edu.snu.spl.cruise.utils.CatchableExecutors;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The driver for central communication service example.
 * Launch executors which exchange messages with the driver.
 *
 * 1. Each tasklet sends a message to the driver and waits for a response message.
 * 2. When all messages from the tasklets has arrived, the driver sends response messages to the tasklets.
 * 3. All tasklets are terminated by the response messages.
 */
@DriverSide
@Unit
final class ETCentCommExampleDriver {
  private static final String TASKLET_PREFIX = "WorkerTasklet-";

  private final ExecutorConfiguration executorConf;

  private final ETMaster etMaster;
  private final DriverSideMsgHandler driverSideMsgHandler;

  private final int splits;

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
      try {
        final List<AllocatedExecutor> executors = etMaster.addExecutors(splits, executorConf).get();

        CatchableExecutors.newSingleThreadExecutor().submit(() -> {
          // start update tasklets on worker executors
          final AtomicInteger taskletIdCount = new AtomicInteger(0);
          final List<Future<RunningTasklet>> taskletFutureList = new ArrayList<>(executors.size());
          executors.forEach(executor -> taskletFutureList.add(executor.submitTasklet(
              TaskletConfiguration.newBuilder()
                  .setId(TASKLET_PREFIX + taskletIdCount.getAndIncrement())
                  .setTaskletClass(ETCentCommSlaveTask.class)
                  .build())));

          driverSideMsgHandler.sendResponseAfterReceivingAllMsgs();

          TaskletUtils.waitAndCheckTaskletResult(taskletFutureList, true);

          executors.forEach(AllocatedExecutor::close);
        });
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
