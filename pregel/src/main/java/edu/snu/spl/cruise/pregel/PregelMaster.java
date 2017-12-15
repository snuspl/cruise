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
package edu.snu.spl.cruise.pregel;

import edu.snu.spl.cruise.common.centcomm.master.MasterSideCentCommMsgSender;
import edu.snu.spl.cruise.services.et.common.util.TaskletUtils;
import edu.snu.spl.cruise.services.et.configuration.TaskletConfiguration;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedExecutor;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedTable;
import edu.snu.spl.cruise.services.et.driver.impl.RunningTasklet;
import edu.snu.spl.cruise.utils.AvroUtils;
import edu.snu.spl.cruise.utils.ConfigurationUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Pregel master that communicates with workers using CentComm services.
 * It synchronizes all workers in a single superstep by checking messages that all workers have sent.
 */
@DriverSide
final class PregelMaster {
  private static final Logger LOG = Logger.getLogger(PregelMaster.class.getName());
  private static final String WORKER_PREFIX = "Worker-";

  private final MasterSideCentCommMsgSender masterSideCentCommMsgSender;

  private final Set<String> executorIds;

  /**
   * These two values are updated by the results of every worker at the end of a single superstep.
   * Master checks whether 1) all vertices in workers are halt or not, 2) and any ongoing messages are exist or not.
   * When both values are true, {@link PregelMaster} stops workers to finish the job.
   */
  private volatile boolean isAllVerticesHalt;
  private volatile boolean isNoOngoingMsgs;

  private volatile CountDownLatch msgCountDownLatch;

  private final int numWorkers;
  private final AtomicInteger workerCounter = new AtomicInteger(0);
  private final Configuration taskConf;

  @Inject
  private PregelMaster(final MasterSideCentCommMsgSender masterSideCentCommMsgSender,
                       @Parameter(PregelParameters.SerializedTaskletConf.class) final String serializedTaskConf,
                       @Parameter(PregelParameters.NumWorkers.class) final int numWorkers) throws IOException {
    this.masterSideCentCommMsgSender = masterSideCentCommMsgSender;
    this.msgCountDownLatch = new CountDownLatch(numWorkers);
    this.executorIds = Collections.synchronizedSet(new HashSet<String>(numWorkers));
    this.isAllVerticesHalt = true;
    this.isNoOngoingMsgs = true;
    this.numWorkers = numWorkers;
    this.taskConf = ConfigurationUtils.fromString(serializedTaskConf);
    initControlThread();
  }

  public void start(final List<AllocatedExecutor> executors,
                    final AllocatedTable vertexTable,
                    final AllocatedTable msgTable1,
                    final AllocatedTable msgTable2) {
    final List<Future<RunningTasklet>> taskFutureList = new ArrayList<>();
    executors.forEach(executor -> taskFutureList.add(executor.submitTasklet(buildTaskConf())));

    TaskletUtils.waitAndCheckTaskletResult(taskFutureList, true);
  }

  private TaskletConfiguration buildTaskConf() {
    return TaskletConfiguration.newBuilder()
        .setId(WORKER_PREFIX + workerCounter.getAndIncrement())
        .setTaskletClass(PregelWorkerTask.class)
        .setUserParamConf(taskConf)
        .build();
  }

  private void initControlThread() {

    LOG.log(Level.INFO, "Start a thread that controls workers...");
    // submit a runnable that controls workers' supersteps.
    new Thread(() -> {
      while (true) {
        try {
          msgCountDownLatch.await();
        } catch (final InterruptedException e) {
          throw new RuntimeException("Unexpected exception", e);
        }

        final ControlMsgType controlMsgType = isAllVerticesHalt && isNoOngoingMsgs
            ? ControlMsgType.Stop : ControlMsgType.Start;
        final SuperstepControlMsg controlMsg = SuperstepControlMsg.newBuilder()
            .setType(controlMsgType)
            .build();

        executorIds.forEach(executorId -> {
          try {
            masterSideCentCommMsgSender.send(PregelDriver.CENTCOMM_CLIENT_ID, executorId,
                AvroUtils.toBytes(controlMsg, SuperstepControlMsg.class));
          } catch (NetworkException e) {
            throw new RuntimeException(e);
          }
        });

        if (controlMsgType.equals(ControlMsgType.Stop)) {
          break;
        }

        // reset for next superstep
        isAllVerticesHalt = true;
        isNoOngoingMsgs = true;
        msgCountDownLatch = new CountDownLatch(numWorkers);
      }
    }).start();
  }

  /**
   * Handles {@link SuperstepResultMsg} from workers.
   */
  void onWorkerMsg(final String workerId, final SuperstepResultMsg resultMsg) {
    if (!executorIds.contains(workerId)) {
      executorIds.add(workerId);
    }

    LOG.log(Level.INFO, "isAllVerticesHalt : {0}, isNoOngoingMsgs : {1}",
        new Object[]{resultMsg.getIsAllVerticesHalt(), resultMsg.getIsNoOngoingMsgs()});
    isAllVerticesHalt &= resultMsg.getIsAllVerticesHalt();
    isNoOngoingMsgs &= resultMsg.getIsNoOngoingMsgs();

    msgCountDownLatch.countDown();
  }
}
