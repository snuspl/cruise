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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A executor sync manager that communicates with executors using CentComm services.
 * It Provides a way to synchronize all tasks by checking all executors have sent the messages.
 * When all messages from the tasks arrive, it sends response messages to all tasks.
 */
@DriverSide
final class PregelMaster implements EventHandler<CentCommMsg> {
  private static final Logger LOG = Logger.getLogger(PregelMaster.class.getName());

  private final MasterSideCentCommMsgSender masterSideCentCommMsgSender;
  private final AtomicInteger activeVerticesCounter = new AtomicInteger(0);
  private CountDownLatch msgCountDown;
  private Set<String> evaluatorIds;

  private boolean isAllVerticesHalt;

  @Inject
  private PregelMaster(final MasterSideCentCommMsgSender masterSideCentCommMsgSender) {
    this.masterSideCentCommMsgSender = masterSideCentCommMsgSender;
    this.msgCountDown = new CountDownLatch(PregelDriver.NUM_EXECUTORS);
    this.evaluatorIds = Collections.synchronizedSet(new HashSet<String>(PregelDriver.NUM_EXECUTORS));
    initThread();
    isAllVerticesHalt = false;
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

    if (!evaluatorIds.contains(sourceId)) {
      evaluatorIds.add(sourceId);
    }

    final WorkerMsg workerMsg = AvroUtils.fromBytes(message.getData().array(), WorkerMsg.class);

    synchronized (this) {
      isAllVerticesHalt = isAllVerticesHalt || workerMsg.getIsAllVerticesHalt();
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

        final MasterMsgType masterMsgType = isAllVerticesHalt ? MasterMsgType.Stop : MasterMsgType.Start;
        final MasterMsg masterMsg = MasterMsg.newBuilder()
            .setType(masterMsgType)
            .build();

        evaluatorIds.forEach(evaluatorId -> {
          try {
            masterSideCentCommMsgSender.send(PregelDriver.CENTCOMM_CLIENT_ID, evaluatorId,
                AvroUtils.toBytes(masterMsg, MasterMsg.class));
          } catch (NetworkException e) {
            throw new RuntimeException(e);
          }
        });

        if (masterMsgType.equals(MasterMsgType.Stop)) {
          break;
        }

        activeVerticesCounter.set(0);
        isAllVerticesHalt = false;
        msgCountDown = new CountDownLatch(PregelDriver.NUM_EXECUTORS);
      }

    }
  }
}
