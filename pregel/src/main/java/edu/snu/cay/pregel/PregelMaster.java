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
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Pregel master that communicates with workers using CentComm services.
 * It synchronizes all workers in a single superstep by checking messages that all workers have sent.
 */
@DriverSide
final class PregelMaster implements EventHandler<CentCommMsg> {
  private static final Logger LOG = Logger.getLogger(PregelMaster.class.getName());

  private final MasterSideCentCommMsgSender masterSideCentCommMsgSender;

  private final Set<String> executorIds;

  private boolean isAllVerticesHalt;

  private CountDownLatch msgCountDown;

  @Inject
  private PregelMaster(final MasterSideCentCommMsgSender masterSideCentCommMsgSender) {
    this.masterSideCentCommMsgSender = masterSideCentCommMsgSender;
    this.msgCountDown = new CountDownLatch(PregelDriver.NUM_EXECUTORS);
    this.executorIds = Collections.synchronizedSet(new HashSet<String>(PregelDriver.NUM_EXECUTORS));
    isAllVerticesHalt = false;
    initThread();
  }

  private void initThread() {
    LOG.log(Level.INFO, "Start synchronization of executors...");
    final Thread msgManagerThread = new Thread(new MasterMsgManagerThread());
    msgManagerThread.start();
  }

  /**
   * CentComm message handling logic.
   * @param message received CentComm message
   * @throws RuntimeException if the received message is incorrect
   */
  @Override
  public void onNext(final CentCommMsg message) {

    final String sourceId = message.getSourceId().toString();

    LOG.log(Level.INFO, "Received CentComm message {0} from {1}",
        new Object[]{message, sourceId});

    if (!executorIds.contains(sourceId)) {
      executorIds.add(sourceId);
    }

    final SuperstepResultMsg resultMsg = AvroUtils.fromBytes(message.getData().array(), SuperstepResultMsg.class);

    synchronized (this) {
      isAllVerticesHalt = isAllVerticesHalt || resultMsg.getIsAllVerticesHalt();
    }
    msgCountDown.countDown();
  }

  private class MasterMsgManagerThread implements Runnable {

    @Override
    public void run() {
      while (true) {
        try {
          msgCountDown.await();
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
        msgCountDown = new CountDownLatch(PregelDriver.NUM_EXECUTORS);
      }

    }
  }
}
