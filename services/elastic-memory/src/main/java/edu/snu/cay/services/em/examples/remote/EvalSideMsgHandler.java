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
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Evaluator-side message handler.
 * A task waits response from the driver through this class.
 */
@EvaluatorSide
final class EvalSideMsgHandler implements EventHandler<AggregationMessage> {

  private static final Logger LOG = Logger.getLogger(EvalSideMsgHandler.class.getName());

  private final SerializableCodec<String> codec;
  private CountDownLatch latch;

  @Inject
  private EvalSideMsgHandler(final SerializableCodec<String> codec) {
    this.codec = codec;
    this.latch = new CountDownLatch(1);
  }

  @Override
  public void onNext(final AggregationMessage message) {
    final String data = codec.decode(message.getData().array());
    if (!data.equals(DriverSideMsgHandler.MSG_FROM_DRIVER)) {
      throw new RuntimeException(String.format("A wrong data %s was sent from the driver but we expect %s", data,
          DriverSideMsgHandler.MSG_FROM_DRIVER));
    } else {
      LOG.log(Level.INFO, "Message from the driver: {0}", data);
    }

    latch.countDown();
  }

  void waitForMessage() {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Unexpected exception", e);
    }
    latch = new CountDownLatch(1);
  }
}
