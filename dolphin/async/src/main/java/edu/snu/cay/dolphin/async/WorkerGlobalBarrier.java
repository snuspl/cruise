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

import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.impl.utils.ResettingCountDownLatch;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Synchronizes all workers by exchanging synchronization messages with the driver.
 * It is used to synchronize the local worker with other workers in two points: after initialization and before cleanup.
 */
@EvaluatorSide
@NotThreadSafe
final class WorkerGlobalBarrier {
  private static final Logger LOG = Logger.getLogger(WorkerGlobalBarrier.class.getName());

  private volatile StateMachine stateMachine;

  private final ResettingCountDownLatch countDownLatch = new ResettingCountDownLatch(1);

  private final WorkerSideMsgSender workerSideMsgSender;

  @Inject
  private WorkerGlobalBarrier(final WorkerSideMsgSender workerSideMsgSender) {
    this.workerSideMsgSender = workerSideMsgSender;
  }

  enum State {
    INIT,
    RUN,
    CLEANUP
  }
  
  public void init() {
    this.stateMachine = StateMachine.newBuilder()
        .addState(State.INIT, "Workers are initializing themselves")
        .addState(State.RUN, "Workers are running their tasks. Optimization can take place")
        .addState(State.CLEANUP, "Workers are cleaning up the task")
        .addTransition(State.INIT, State.RUN, "The worker init is finished, time to start running task")
        .addTransition(State.RUN, State.CLEANUP, "The task execution is finished, time to clean up the task")
        .setInitialState(State.INIT)
        .build();
  }

  /**
   * Worker waits on a global synchronization barrier.
   * When all threads have been observed at the barrier,
   * it sends a synchronization message to the driver and waits until
   * a response message arrives from the driver.
   * After receiving the reply, this {@link WorkerGlobalBarrier} releases worker to progress.
   */
  void await() throws NetworkException {
    final State currentState = (State) stateMachine.getCurrentState();
    LOG.log(Level.INFO, "Start waiting other workers to reach barrier: {0}", currentState);

    switch (currentState) {
    case INIT:
    case RUN:
      break;
    case CLEANUP:
    default:
      throw new RuntimeException("Invalid state: await() should not be called in the CLEANUP state");
    }

    LOG.log(Level.INFO, "Sending a synchronization message to the driver");
    workerSideMsgSender.sendSyncMsg((State) stateMachine.getCurrentState());

    countDownLatch.awaitAndReset(1);
    LOG.log(Level.INFO, "Release from barrier");
  }

  /**
   * Progress to the next state.
   */
  private void transitToNextState() {
    final State currentState = (State) stateMachine.getCurrentState();

    switch (currentState) {
    case INIT:
      stateMachine.setState(State.RUN);
      LOG.log(Level.INFO, String.format("State transition: %s -> %s", State.INIT, State.RUN));
      break;
    case RUN:
      stateMachine.setState(State.CLEANUP);
      LOG.log(Level.INFO, String.format("State transition: %s -> %s", State.RUN, State.CLEANUP));
      break;
    case CLEANUP:
      throw new RuntimeException(String.format("No more transition is allowed after %s state", State.CLEANUP));
    default:
      throw new RuntimeException(String.format("Invalid state: %s", currentState));
    }
  }

  /**
   * Handles release msgs from driver.
   */
  synchronized void onReleaseMsg() {
    LOG.log(Level.FINE, "Received a response message from the driver");

    transitToNextState();
    countDownLatch.countDown();
  }
}
