/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.em.examples.remote;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-side message handler that receives aggregation messages as an aggregation master.
 * Provides a way to synchronize all worker tasks by checking all workers have sent the messages.
 * It sends response messages to all tasks when all messages from the tasks arrive.
 */
@DriverSide
final class DriverSideMsgHandler implements EventHandler<AggregationMessage> {

  private static final Logger LOG = Logger.getLogger(DriverSideMsgHandler.class.getName());

  static final String MSG_FROM_DRIVER = "MSG_FROM_DRIVER";

  private final AggregationMaster aggregationMaster;
  private final Codec<String> codec;
  private CountDownLatch msgCountDown;
  private final Set<String> workerIds;

  @Inject
  private DriverSideMsgHandler(final AggregationMaster aggregationMaster,
                               final SerializableCodec<String> codec) {
    this.aggregationMaster = aggregationMaster;
    this.codec = codec;
    this.msgCountDown = new CountDownLatch(RemoteEMDriver.EVAL_NUM);
    this.workerIds = Collections.synchronizedSet(new HashSet<String>(RemoteEMDriver.EVAL_NUM));
    syncWorkers();
  }

  /**
   * Aggregation message handling logic.
   * @param message received aggregation message
   * @throws RuntimeException if the received message is incorrect
   */
  @Override
  public void onNext(final AggregationMessage message) {
    LOG.log(Level.INFO, "Received aggregation message {0}", message);
    final String workerId = message.getSourceId().toString();
    final String data = codec.decode(message.getData().array());

    if (!workerIds.contains(workerId)) {
      workerIds.add(workerId);
    }

    if (validateMsg(workerId, data)) {
      msgCountDown.countDown();
    } else {
      throw new RuntimeException(String.format("WorkerId %s should not send message with data %s.", workerId, data));
    }
  }

  /**
   * Checks the validity of messages sent from workers.
   * It checks that workerId of the message is CONTEXT_ID_PREFIX + index,
   * and data of the message is TASK_ID_PREFIX + index.
   *
   * @param workerId an id of worker
   * @param data a data sent from worker
   * @return true if the msg is valid
   */
  private boolean validateMsg(final String workerId, final String data) {
    return workerId.startsWith(RemoteEMDriver.CONTEXT_ID_PREFIX)
        && data.startsWith(RemoteEMDriver.TASK_ID_PREFIX)
        && workerId.substring(RemoteEMDriver.CONTEXT_ID_PREFIX.length())
        .equals(data.substring(RemoteEMDriver.TASK_ID_PREFIX.length()));
  }

  /**
   * Start synchronizing workers by executing a thread controlling workers.
   */
  private void syncWorkers() {
    LOG.log(Level.INFO, "Start synchronization of workers...");
    final Thread syncThread = new Thread(new SyncThread());
    syncThread.start();
  }

  /**
   * A Runnable that sends a response message to workers, when all they sent a message to driver.
   * Workers can match their progress with each other by waiting driver's response.
   */
  private class SyncThread implements Runnable {

    @Override
    public void run() {
      while (true) {

        // wait until all workers send a message
        try {
          msgCountDown.await();
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }

        // send response message to all workers
        for (final String slaveId : workerIds) {
          LOG.log(Level.INFO, "Sending a message to {0}", slaveId);
          aggregationMaster.send(RemoteEMDriver.AGGREGATION_CLIENT_ID, slaveId, codec.encode(MSG_FROM_DRIVER));
        }

        // reset latch for next iterations
        msgCountDown = new CountDownLatch(RemoteEMDriver.EVAL_NUM);
      }
    }
  }
}
