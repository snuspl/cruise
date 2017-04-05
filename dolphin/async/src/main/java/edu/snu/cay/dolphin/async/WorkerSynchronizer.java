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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.common.aggregation.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.slave.AggregationSlave;
import edu.snu.cay.dolphin.async.SynchronizationManager.State;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.group.impl.utils.ResettingCountDownLatch;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Synchronizes all workers by exchanging synchronization messages with the driver.
 * It is used to synchronize the local worker threads themselves and
 * with other workers in two points: after initialization and before cleanup.
 * It maintains a local state for enabling workers newly added by EM to bypass the initial barrier.
 */
@EvaluatorSide
@Unit
final class WorkerSynchronizer {
  private static final Logger LOG = Logger.getLogger(WorkerSynchronizer.class.getName());

  private final StateMachine localStateMachine;

  private final ResettingCountDownLatch countDownLatch = new ResettingCountDownLatch(1);
  private final CyclicBarrier cyclicBarrier;

  @Inject
  private WorkerSynchronizer(final AggregationSlave aggregationSlave,
                             final SerializableCodec<Enum> codec) {
    this.localStateMachine = SynchronizationManager.initStateMachine();
    // TODO #681: Need to add numWorkerThreads concept after multi-thread worker is enabled
    this.cyclicBarrier = new CyclicBarrier(1, new Runnable() {
      @Override
      public void run() {
        LOG.log(Level.INFO, "Sending a synchronization message to the driver");

        final byte[] data = codec.encode(localStateMachine.getCurrentState());
        aggregationSlave.send(SynchronizationManager.AGGREGATION_CLIENT_NAME, data);
        countDownLatch.awaitAndReset(1);
      }
    });
  }

  /**
   * All worker threads wait on a local synchronization barrier.
   * When all threads have been observed at the barrier,
   * the {@link WorkerSynchronizer} sends a single synchronization message to the driver and blocks until
   * a response message arrives from the driver.
   * After receiving the reply, this {@link WorkerSynchronizer} releases all threads from the barrier.
   */
  void globalBarrier() {
    final State currentState = (State) localStateMachine.getCurrentState();

    switch (currentState) {
    case INIT:
    case RUN:
      break;
    case CLEANUP:
      throw new RuntimeException("Workers never call the global barrier in cleanup");
    default:
      throw new RuntimeException("Invalid state");
    }

    try {
      cyclicBarrier.await();
    } catch (final InterruptedException | BrokenBarrierException e) {
      throw new RuntimeException(e);
    }
  }

  final class MessageHandler implements EventHandler<CentCommMsg> {

    @Override
    public void onNext(final CentCommMsg aggregationMessage) {
      LOG.log(Level.INFO, "Received a response message from the driver");

      SynchronizationManager.transitState(localStateMachine);
      countDownLatch.countDown();
    }
  }
}
