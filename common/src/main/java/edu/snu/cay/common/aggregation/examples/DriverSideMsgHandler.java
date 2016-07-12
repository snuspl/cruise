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
package edu.snu.cay.common.aggregation.examples;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
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

/**
 * Driver-side message handler.
 * This receives aggregation messages, since driver is an aggregation master.
 * Provides a way to check whether all messages have arrived or not via {@code validate()} method.
 * It sends response messages to all tasks when all messages from the tasks arrive.
 */
@DriverSide
public final class DriverSideMsgHandler implements EventHandler<AggregationMessage> {

  private static final Logger LOG = Logger.getLogger(DriverSideMsgHandler.class.getName());

  public static final String MSG_FROM_DRIVER = "MSG_FROM_DRIVER";

  private final AggregationMaster aggregationMaster;
  private final Codec<String> codec;
  private final CountDownLatch msgCountDown;
  private final Set<String> slaveIds;

  @Inject
  private DriverSideMsgHandler(final AggregationMaster aggregationMaster,
                               @Parameter(Parameters.Splits.class) final int split,
                               final SerializableCodec<String> codec) {
    this.aggregationMaster = aggregationMaster;
    this.codec = codec;
    this.msgCountDown = new CountDownLatch(split);
    this.slaveIds = Collections.synchronizedSet(new HashSet<String>(split));
  }

  /**
   * Aggregation message handling logic.
   * @param message received aggregation message
   * @throws RuntimeException if the received message is incorrect
   */
  @Override
  public void onNext(final AggregationMessage message) {
    LOG.log(Level.INFO, "Received aggregation message {0}", message);
    final String slaveId = message.getSourceId().toString();
    final String data = codec.decode(message.getData().array());

    if (slaveIds.contains(slaveId)) {
      throw new RuntimeException("Multiple messages were sent from " + slaveId);
    } else {
      slaveIds.add(slaveId);
    }

    // checks that slaveId of the message is WORKER_CONTEXT_PREFIX + index,
    // and data of the message is TASK_PREFIX + index.
    if (slaveId.startsWith(AggregationExampleDriver.WORKER_CONTEXT_PREFIX)
        && data.startsWith(AggregationExampleDriver.TASK_PREFIX)
        && slaveId.substring(AggregationExampleDriver.WORKER_CONTEXT_PREFIX.length())
        .equals(data.substring(AggregationExampleDriver.TASK_PREFIX.length()))) {
      msgCountDown.countDown();
    } else {
      throw new RuntimeException(String.format("SlaveId %s should not send message with data %s.", slaveId, data));
    }
  }

  /**
   * Checks whether all messages have arrived correctly or not.
   * If all messages have not arrived yet, wait until those messages are received
   * or an interruption(e.g., timeout) occurs. It sends response messages when all messages are
   * successfully received.
   *
   * @throws RuntimeException if an interruption occurs before finish receiving all messages
   */
  public void validate() {
    LOG.log(Level.INFO, "Validating...");

    try {
      msgCountDown.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }

    for (final String slaveId : slaveIds) {
      LOG.log(Level.INFO, "Sending a message to {0}", slaveId);
      try {
        aggregationMaster.send(AggregationExampleDriver.AGGREGATION_CLIENT_ID, slaveId, codec.encode(MSG_FROM_DRIVER));
      } catch (final NetworkException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
