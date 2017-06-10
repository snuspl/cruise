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

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.master.MasterSideCentCommMsgSender;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Pregel master that communicates with workers using CentComm services.
 * It synchronizes all workers in a single superstep by checking messages that all workers have sent.
 */
@Unit
@DriverSide
final class PregelMaster {
  private static final Logger LOG = Logger.getLogger(PregelMaster.class.getName());

  private final MasterSideCentCommMsgSender masterSideCentCommMsgSender;

  private final Set<String> executorIds;

  /**
   * This value is updated by the results of every worker at the end of a single superstep.
   * And it determines whether {@link PregelMaster} starts next superstep or not.
   */
  private volatile boolean isAllVerticesHalt;

  private volatile CountDownLatch msgCountDownLatch;

  @Inject
  private PregelMaster(final MasterSideCentCommMsgSender masterSideCentCommMsgSender) {
    this.masterSideCentCommMsgSender = masterSideCentCommMsgSender;
    this.msgCountDownLatch = new CountDownLatch(PregelDriver.NUM_EXECUTORS);
    this.executorIds = Collections.synchronizedSet(new HashSet<String>(PregelDriver.NUM_EXECUTORS));
    isAllVerticesHalt = false;
    initControlThread();
  }

  private void initControlThread() {
    LOG.log(Level.INFO, "Start a thread that controls workers...");
    final ExecutorService executor = Executors.newSingleThreadExecutor();

    // submit a runnable that controls workers' supersteps.
    executor.submit((Runnable) () -> {
      while (true) {
        try {
          msgCountDownLatch.await();
        } catch (final InterruptedException e) {
          throw new RuntimeException("Unexpected exception", e);
        }

        final ControlMsgType controlMsgType = isAllVerticesHalt ? ControlMsgType.Stop : ControlMsgType.Start;
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
        isAllVerticesHalt = false;
        msgCountDownLatch = new CountDownLatch(PregelDriver.NUM_EXECUTORS);
      }
    });
  }

  /**
   * Handles {@link SuperstepResultMsg} from workers.
   */
  final class MasterMsgHandler implements EventHandler<CentCommMsg> {

    @Override
    public void onNext(final CentCommMsg message) {

      final String sourceId = message.getSourceId().toString();

      LOG.log(Level.INFO, "Received CentComm message {0} from {1}",
          new Object[]{message, sourceId});

      if (!executorIds.contains(sourceId)) {
        executorIds.add(sourceId);
      }

      final SuperstepResultMsg resultMsg = AvroUtils.fromBytes(message.getData().array(), SuperstepResultMsg.class);

      isAllVerticesHalt = isAllVerticesHalt || resultMsg.getIsAllVerticesHalt();

      msgCountDownLatch.countDown();
    }
  }
}
