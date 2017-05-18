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
import edu.snu.cay.common.centcomm.slave.SlaveSideCentCommMsgSender;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A component for an executor to synchronize with other executors.
 * By calling {@link #waitForTryNextSuperstepMsg(int)}, it sends a message to {@link PregelMaster} and waits a response.
 */
@EvaluatorSide
final class WorkerMsgManager implements EventHandler<CentCommMsg> {
  private static final Logger LOG = Logger.getLogger(WorkerMsgManager.class.getName());

  private final SlaveSideCentCommMsgSender centCommMsgSender;
  private boolean goNextsuperstep = true;
  private CountDownLatch latch;

  @Inject
  private WorkerMsgManager(final SlaveSideCentCommMsgSender centCommMsgSender) {
    this.centCommMsgSender = centCommMsgSender;
    this.latch = new CountDownLatch(1);
  }

  @Override
  public void onNext(final CentCommMsg message) {
    LOG.log(Level.INFO, "Received CentComm message {0}", message);
    final MasterMsg masterMsg = AvroUtils.fromBytes(message.getData().array(), MasterMsg.class);
    onMsgMasterMsg(masterMsg);
    latch.countDown();
  }

  private void onMsgMasterMsg(final MasterMsg msg) {
    switch (msg.getType()) {
    case Start:
      goNextsuperstep = true;
      break;
    case Stop:
      goNextsuperstep = false;
      break;
    default:
      break;
    }
  }
  /**
   * Synchronize with other executors.
   * It sends a message to master and waits a response message.
   *
   * @param numActiveVertices the number of active vertices
   */
  boolean waitForTryNextSuperstepMsg(final int numActiveVertices) {

    final boolean isAllVerticesHalt = numActiveVertices == 0;
    final WorkerMsg workerMsg = WorkerMsg.newBuilder()
        .setIsAllVerticesHalt(isAllVerticesHalt)
        .build();

    centCommMsgSender.send(PregelDriver.CENTCOMM_CLIENT_ID, AvroUtils.toBytes(workerMsg, WorkerMsg.class));

    try {
      latch.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Unexpected exception", e);
    }

    // reset for next waitForTryNextSuperstepMsg
    latch = new CountDownLatch(1);
    return goNextsuperstep;
  }
}
