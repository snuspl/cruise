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
package edu.snu.cay.services.et.examples.userservice;

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.master.MasterSideCentCommMsgSender;
import edu.snu.cay.common.param.Parameters;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.et.examples.userservice.ETCentCommExample.CENT_COMM_CLIENT_ID;

/**
 * Driver-side message handler that aggregates msgs from executors.
 * It provides a way to check whether all messages have arrived or not via {@code validate()} method.
 * It sends response messages to all tasks when all messages from the tasks arrive.
 */
@DriverSide
public final class DriverSideMsgHandler implements EventHandler<CentCommMsg> {

  private static final Logger LOG = Logger.getLogger(DriverSideMsgHandler.class.getName());

  static final String MSG_FROM_DRIVER = "MSG_FROM_DRIVER";

  private final MasterSideCentCommMsgSender centCommMsgSender;
  private final Codec<String> codec;
  private final CountDownLatch msgCountDown;
  private final Set<String> slaveIds;

  @Inject
  private DriverSideMsgHandler(final MasterSideCentCommMsgSender centCommMsgSender,
                               @Parameter(Parameters.Splits.class) final int splits,
                               final SerializableCodec<String> codec) {
    this.centCommMsgSender = centCommMsgSender;
    this.codec = codec;
    this.msgCountDown = new CountDownLatch(splits);
    this.slaveIds = Collections.synchronizedSet(new HashSet<String>(splits));
  }

  /**
   * CentComm message handling logic.
   * @param message received centComm message
   * @throws RuntimeException if the received message is incorrect
   */
  @Override
  public void onNext(final CentCommMsg message) {
    final String slaveId = message.getSourceId().toString();
    final String data = codec.decode(message.getData().array());

    LOG.log(Level.INFO, "Received centComm message from {0}. data: {1}", new Object[]{slaveId, data});

    if (slaveIds.contains(slaveId)) {
      throw new RuntimeException("Multiple messages were sent from " + slaveId);
    } else {
      slaveIds.add(slaveId);
    }

    msgCountDown.countDown();
  }

  /**
   * Checks whether all messages have arrived correctly or not.
   * If all messages have not arrived yet, wait until those messages are received
   * or an interruption(e.g., timeout) occurs. It sends response messages when all messages are
   * successfully received.
   *
   * @throws RuntimeException if an interruption occurs before finish receiving all messages
   */
  void sendResponseAfterReceivingAllMsgs() {
    LOG.log(Level.INFO, "Waiting...");

    try {
      msgCountDown.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }

    for (final String slaveId : slaveIds) {
      LOG.log(Level.INFO, "Sending a message to {0}", slaveId);
      try {
        centCommMsgSender.send(CENT_COMM_CLIENT_ID, slaveId, codec.encode(MSG_FROM_DRIVER));
      } catch (final NetworkException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
