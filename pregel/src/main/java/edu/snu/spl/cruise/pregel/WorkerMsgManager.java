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

import edu.snu.spl.cruise.common.centcomm.avro.CentCommMsg;
import edu.snu.spl.cruise.common.centcomm.slave.SlaveSideCentCommMsgSender;
import edu.snu.spl.cruise.utils.AvroUtils;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A component for an executor to synchronize with other executors.
 * By calling {@link #waitForTryNextSuperstepMsg}, it sends a message to {@link PregelMaster} and waits a response.
 * Master will decide whether the worker continues or not.
 */
@EvaluatorSide
final class WorkerMsgManager implements EventHandler<CentCommMsg> {
  private static final Logger LOG = Logger.getLogger(WorkerMsgManager.class.getName());

  private final SlaveSideCentCommMsgSender centCommMsgSender;

  /**
   * This value is updated by the response from master at the end of each superstep.
   * If it's true worker stops running supersteps.
   */
  private volatile boolean goNextSuperstep;

  private volatile CountDownLatch syncLatch;

  @Inject
  private WorkerMsgManager(final SlaveSideCentCommMsgSender centCommMsgSender) {
    this.centCommMsgSender = centCommMsgSender;
  }

  @Override
  public void onNext(final CentCommMsg message) {
    LOG.log(Level.INFO, "Received CentComm message {0}", message);
    final SuperstepControlMsg controlMsg = AvroUtils.fromBytes(message.getData().array(), SuperstepControlMsg.class);
    onControlMsg(controlMsg);
  }

  private void onControlMsg(final SuperstepControlMsg msg) {
    switch (msg.getType()) {
    case Start:
      goNextSuperstep = true;
      break;
    case Stop:
      goNextSuperstep = false;
      break;
    default:
      throw new RuntimeException("unexpected type");
    }
    syncLatch.countDown();
  }

  /**
   * Synchronize with other executors.
   * It sends a message to master and waits a response message.
   *
   * @param numActiveVertices the number of active vertices after superstep
   * @param numSentMsgs the number of sent messages in this superstep
   */
  boolean waitForTryNextSuperstepMsg(final int numActiveVertices, final int numSentMsgs) {

    // 1. reset state
    this.goNextSuperstep = false;
    this.syncLatch = new CountDownLatch(1);

    // 2. send a message
    final boolean isAllVerticesHalt = numActiveVertices == 0;
    final boolean isNoOngoingMsgs = numSentMsgs == 0;
    final SuperstepResultMsg resultMsg = SuperstepResultMsg.newBuilder()
        .setIsAllVerticesHalt(isAllVerticesHalt)
        .setIsNoOngoingMsgs(isNoOngoingMsgs)
        .build();

    centCommMsgSender.send(PregelDriver.CENTCOMM_CLIENT_ID, AvroUtils.toBytes(resultMsg, SuperstepResultMsg.class));

    // 3. wait for a response
    try {
      syncLatch.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Unexpected exception", e);
    }

    return goNextSuperstep;
  }
}
