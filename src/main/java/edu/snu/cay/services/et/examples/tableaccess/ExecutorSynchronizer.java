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
package edu.snu.cay.services.et.examples.tableaccess;

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.slave.SlaveSideCentCommMsgSender;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A component for an executor to synchronize with other executors.
 * By calling {@link #sync()}, it sends a message to {@link ExecutorSyncManager} and waits a response.
 */
@EvaluatorSide
final class ExecutorSynchronizer implements EventHandler<CentCommMsg> {
  private static final Logger LOG = Logger.getLogger(ExecutorSynchronizer.class.getName());
  private static final byte[] EMPTY_DATA = new byte[0];

  private final SlaveSideCentCommMsgSender centCommMsgSender;
  private CountDownLatch latch;

  @Inject
  private ExecutorSynchronizer(final SlaveSideCentCommMsgSender centCommMsgSender) {
    this.centCommMsgSender = centCommMsgSender;
    this.latch = new CountDownLatch(1);
  }

  @Override
  public void onNext(final CentCommMsg message) {
    LOG.log(Level.INFO, "Received CentComm message {0}", message);
    latch.countDown();
  }

  /**
   * Synchronize with other executors.
   * It sends a message to master and waits a response message.
   */
  void sync() {
    centCommMsgSender.send(TableAccessETDriver.CENTCOMM_CLIENT_ID, EMPTY_DATA);

    try {
      latch.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Unexpected exception", e);
    }

    // reset for next sync
    latch = new CountDownLatch(1);
  }
}
