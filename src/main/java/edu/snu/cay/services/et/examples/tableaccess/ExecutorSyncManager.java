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
import edu.snu.cay.common.centcomm.master.MasterSideCentCommMsgSender;
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
 * A executor sync manager that communicates with executors using CentComm services.
 * It Provides a way to synchronize all tasks by checking all executors have sent the messages.
 * When all messages from the tasks arrive, it sends response messages to all tasks.
 */
@DriverSide
final class ExecutorSyncManager implements EventHandler<CentCommMsg> {
  private static final Logger LOG = Logger.getLogger(ExecutorSyncManager.class.getName());
  private static final byte[] EMPTY_DATA = new byte[0];

  private final MasterSideCentCommMsgSender masterSideCentCommMsgSender;
  private volatile CountDownLatch msgCountDownLatch;
  private final Set<String> executorIds;

  @Inject
  private ExecutorSyncManager(final MasterSideCentCommMsgSender masterSideCentCommMsgSender) {
    this.masterSideCentCommMsgSender = masterSideCentCommMsgSender;
    this.msgCountDownLatch = new CountDownLatch(TableAccessETDriver.NUM_EXECUTORS);
    this.executorIds = Collections.synchronizedSet(new HashSet<>(TableAccessETDriver.NUM_EXECUTORS));
    initSyncThread();
  }

  /**
   * Start a thread that synchronizes executors.
   */
  private void initSyncThread() {
    LOG.log(Level.INFO, "Start synchronization of executors...");
    final Thread syncThread = new Thread(new SyncThread());
    syncThread.start();
  }

  /**
   * CentComm message handling logic.
   * @param message received CentComm message
   * @throws RuntimeException if the received message is incorrect
   */
  @Override
  public synchronized void onNext(final CentCommMsg message) {
    final String executorId = message.getSourceId().toString();
    LOG.log(Level.INFO, "Received CentComm message {0} from {1}", new Object[]{message, executorId});

    // collect executor ids
    executorIds.add(executorId);

    msgCountDownLatch.countDown();
  }

  /**
   * A Runnable that sends a response message to executors, when all they have sent a message to driver.
   * Workers can match their progress with each other by waiting the response.
   */
  private class SyncThread implements Runnable {

    @Override
    public void run() {
      while (true) {

        // wait until all executors send a message
        while (true) {
          try {
            msgCountDownLatch.await();
            break;
          } catch (final InterruptedException e) {
            // ignore and keep waiting
          }
        }

        // reset for next iteration
        msgCountDownLatch = new CountDownLatch(TableAccessETDriver.NUM_EXECUTORS);

        // send response message to all executors
        sendResponseToExecutors();
      }
    }

    private void sendResponseToExecutors() {
      final Set<String> executorsToRelease = new HashSet<>(executorIds);
      executorIds.clear();

      for (final String executorId : executorsToRelease) {
        LOG.log(Level.INFO, "Sending a message to {0}", executorId);
        try {
          masterSideCentCommMsgSender.send(TableAccessETDriver.CENTCOMM_CLIENT_ID, executorId, EMPTY_DATA);
        } catch (final NetworkException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
