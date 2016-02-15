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
import edu.snu.cay.common.param.Parameters;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-side message handler.
 * This receives aggregation messages, since driver is an aggregation master.
 * Provides a way to check whether all messages are arrived or not via {@code validate()} method.
 */
@DriverSide
public final class DriverSideMsgHandler implements EventHandler<AggregationMessage> {
  private static final Logger LOG = Logger.getLogger(DriverSideMsgHandler.class.getName());

  private final Codec<String> codec;
  private final CountDownLatch msgCountDown;
  private RuntimeException exception = null;

  @Inject
  private DriverSideMsgHandler(@Parameter(Parameters.Splits.class) final int split,
                               final SerializableCodec<String> codec) {
    this.codec = codec;
    this.msgCountDown = new CountDownLatch(split);
  }

  @Override
  public void onNext(final AggregationMessage message) {
    LOG.log(Level.INFO, "Received aggregation message {0}", message);
    final String slaveId = message.getSlaveId().toString();
    final String data = codec.decode(message.getData().array());

    // checks that slaveId of the message is WORKER_CONTEXT_PREFIX + index,
    // and data of the message is TASK_PREFIX + index.
    if (slaveId.substring(AggregationExampleDriver.WORKER_CONTEXT_PREFIX.length())
        .equals(data.substring(AggregationExampleDriver.TASK_PREFIX.length()))) {
      msgCountDown.countDown();
    } else {
      exception = exception == null
          ? new RuntimeException(String.format("SlaveId %s should not send message with data %s.", slaveId, data))
          : exception;
    }
  }

  /**
   * Checks whether all messages are arrived correctly or not.
   * If all messages are not arrived yet, wait until those messages are received
   * or an interruption(e.g., timeout) occurs.
   * @throws RuntimeException if incorrect message is arrived or
   * an interruption occurs before finish receiving all messages
   */
  public void validate() {
    LOG.log(Level.INFO, "Validating...");

    if (exception != null) {
      throw exception;
    }

    try {
      msgCountDown.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
