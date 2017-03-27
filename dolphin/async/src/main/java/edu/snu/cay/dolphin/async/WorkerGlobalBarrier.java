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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.group.impl.utils.ResettingCountDownLatch;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.WorkerStateManager.*;

/**
 * Synchronizes all workers by exchanging synchronization messages with the driver.
 * It is used to synchronize the local worker with other workers in two points: after initialization and before cleanup.
 */
@EvaluatorSide
@Unit
final class WorkerGlobalBarrier {
  private static final Logger LOG = Logger.getLogger(WorkerGlobalBarrier.class.getName());

  private final StateMachine stateMachine;

  private final ResettingCountDownLatch countDownLatch = new ResettingCountDownLatch(1);

  private final AggregationSlave aggregationSlave;
  private final SerializableCodec<Enum> codec;

  @Inject
  private WorkerGlobalBarrier(final AggregationSlave aggregationSlave,
                              final SerializableCodec<Enum> codec) {
    this.stateMachine = initStateMachine();
    this.aggregationSlave = aggregationSlave;
    this.codec = codec;
  }

  private void sendMsgToDriver() {
    LOG.log(Level.INFO, "Sending a synchronization message to the driver");
    final byte[] data = codec.encode(stateMachine.getCurrentState());
    aggregationSlave.send(WorkerStateManager.AGGREGATION_CLIENT_NAME, data);
  }

  /**
   * Worker waits on a global synchronization barrier.
   * When all threads have been observed at the barrier,
   * It sends sends a synchronization message to the driver and blocks until
   * a response message arrives from the driver.
   * After receiving the reply, this {@link WorkerGlobalBarrier} releases worker to progress.
   */
  void await() {
    final State currentState = (State) stateMachine.getCurrentState();

    switch (currentState) {
    case INIT:
    case RUN:
      break;
    case CLEANUP:
    default:
      throw new RuntimeException("Invalid state");
    }

    sendMsgToDriver();
    countDownLatch.awaitAndReset(1);
  }

  final class MessageHandler implements EventHandler<AggregationMessage> {

    @Override
    public synchronized void onNext(final AggregationMessage aggregationMessage) {
      LOG.log(Level.INFO, "Received a response message from the driver");

      transitState(stateMachine);
      countDownLatch.countDown();
    }
  }
}
