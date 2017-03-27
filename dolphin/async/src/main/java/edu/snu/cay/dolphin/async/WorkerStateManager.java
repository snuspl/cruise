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
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A driver-side component that coordinates synchronization between the driver and workers.
 * It is used to synchronize workers in two points: after initialization (STATE_INIT -> STATE_RUN)
 * and before cleanup (STATE_RUN -> STATE_CLEANUP).
 * To achieve this, it maintains a global state to be matched with their own local states.
 */
@DriverSide
@Unit
final class WorkerStateManager {
  private static final Logger LOG = Logger.getLogger(WorkerStateManager.class.getName());

  private static final byte[] EMPTY_DATA = new byte[0];

  static final String AGGREGATION_CLIENT_NAME = WorkerStateManager.class.getName();

  private final AggregationMaster aggregationMaster;

  private final Codec<State> codec;

  @GuardedBy("this")
  private final StateMachine stateMachine;

  /**
   * A set of ids of workers to be synchronized.
   */
  private final Set<String> workerIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

  /**
   * A set maintaining worker ids of whom have sent a sync msg for the barrier.
   */
  @GuardedBy("this")
  private final Set<String> blockedWorkerIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Inject
  private WorkerStateManager(final AggregationMaster aggregationMaster,
                             final SerializableCodec<State> codec) {
    this.aggregationMaster = aggregationMaster;
    this.codec = codec;
    this.stateMachine = initStateMachine();
  }

  enum State {
    INIT,
    RUN,
    CLEANUP
  }

  static StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(State.INIT, "Workers are initializing themselves")
        .addState(State.RUN, "Workers are running their tasks")
        .addState(State.CLEANUP, "Workers are cleaning up the task")
        .addTransition(State.INIT, State.RUN, "The worker init is finished, time to start running task")
        .addTransition(State.RUN, State.CLEANUP, "The task execution is finished, time to clean up the task")
        .setInitialState(State.INIT)
        .build();
  }

  /**
   * Progress to the next state.
   */
  static void transitState(final StateMachine stateMachine) {
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
   * Release all blocked workers.
   */
  private synchronized void releaseWorkers() {
    LOG.log(Level.INFO, "Send response message to {0} blocked workerIds: {1}",
        new Object[]{blockedWorkerIds.size(), blockedWorkerIds});
    // broadcast responses to blocked workerIds
    for (final String workerId : blockedWorkerIds) {
      sendResponseMessage(workerId);
    }

    blockedWorkerIds.clear();
  }

  private void sendResponseMessage(final String workerId) {
    try {
      aggregationMaster.send(AGGREGATION_CLIENT_NAME, workerId, EMPTY_DATA);
    } catch (final NetworkException e) {
      LOG.log(Level.INFO, String.format("Fail to send msg to worker %s.", workerId), e);
    }
  }

  /**
   * Blocks this worker. It can be released by {@link #releaseWorkers()} later.
   * @param workerId a worker id
   * @return True if all workers are blocked
   */
  private synchronized boolean blockWorker(final String workerId) {
    blockedWorkerIds.add(workerId);
    LOG.log(Level.INFO, "Receive a synchronization message from {0}. {1} messages have been received out of {2}.",
        new Object[]{workerId, blockedWorkerIds.size(), blockedWorkerIds.size()});

    return blockedWorkerIds.containsAll(workerIds);
  }

  /**
   * Handles messages from workers.
   * @param workerId a worker id
   * @param localState a state of worker
   */
  private synchronized void onWorkerMsg(final String workerId, final State localState) {
    final State globalState = (State) stateMachine.getCurrentState();

    switch (globalState) {
    case INIT:
      if (localState.equals(State.INIT)) {
        workerIds.add(workerId);

        // worker finishes their initialization and is waiting for response to enter the run stage
        if (blockWorker(workerId)) {
          transitState(stateMachine);
          releaseWorkers();
        }
      } else {
        throw new RuntimeException(String.format("Worker %s is in invalid state: %s", workerId, localState));
      }
      break;
    case RUN:
      if (localState.equals(State.RUN)) {
        // worker finishes their main iteration and is waiting for response to enter the cleanup stage
        if (blockWorker(workerId)) {
          transitState(stateMachine);
          releaseWorkers();
        }
      } else {
        throw new RuntimeException(String.format("Worker %s is in invalid state: %s", workerId, localState));
      }
      break;
    case CLEANUP:
      throw new RuntimeException(String.format("Workers should not send message in %s state", globalState));
    default:
      throw new RuntimeException("Invalid state");
    }
  }

  final class MessageHandler implements EventHandler<AggregationMessage> {

    @Override
    public void onNext(final AggregationMessage aggregationMessage) {
      final String workerId = aggregationMessage.getSourceId().toString();
      final State localState = codec.decode(aggregationMessage.getData().array());

      onWorkerMsg(workerId, localState);
    }
  }
}
