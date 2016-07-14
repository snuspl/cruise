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

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A driver-side component that coordinates synchronization messages between the driver and workers.
 * It is used to synchronize workers in two points: after initialization (STATE_INIT -> STATE_RUN)
 * and before cleanup (STATE_RUN -> STATE CLEANUP).
 * To achieve this, it maintains a global state that all workers should match with their own local states.
 * Workers added by EM can bypass barriers if their state is behind the global state.
 */
@DriverSide
@Unit
final class SynchronizationManager {
  private static final Logger LOG = Logger.getLogger(SynchronizationManager.class.getName());

  private static final byte[] EMPTY_DATA = new byte[0];

  static final String AGGREGATION_CLIENT_NAME = SynchronizationManager.class.getName();

  static final String STATE_INIT = "INIT";
  static final String STATE_RUN = "RUN";
  static final String STATE_CLEANUP = "CLEANUP";

  private final AggregationMaster aggregationMaster;

  private final Codec<String> codec;

  private final StateMachine globalStateMachine;

  private final CountDownLatch initLatch = new CountDownLatch(1);

  /**
   * The total number of workers to sync.
   */
  private int numWorkersToSync = 0;

  /**
   * A set that maintains workers that have sent a sync msg for the current barrier.
   */
  private final Set<String> blockedWorkerIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Inject
  private SynchronizationManager(final AggregationMaster aggregationMaster,
                                 final SerializableCodec<String> codec) {
    this.aggregationMaster = aggregationMaster;
    this.codec = codec;
    this.globalStateMachine = initStateMachine();
  }

  static StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(STATE_INIT, "Workers are initializing themselves")
        .addState(STATE_RUN, "Workers are running their tasks")
        .addState(STATE_CLEANUP, "Workers are cleaning up the task")
        .addTransition(STATE_INIT, STATE_RUN, "The initialization is finished, time to start running task")
        .addTransition(STATE_RUN, STATE_CLEANUP, "The task execution is finished, time to clean up the task")
        .setInitialState(STATE_INIT)
        .build();
  }

  /**
   * Embraces the newly added worker to be synchronized from now.
   */
  synchronized void onWorkerAdded() {
    // increase the number of workers to block
    numWorkersToSync++;
    LOG.log(Level.FINE, "Total number of workers participating in the synchronization = {0}", numWorkersToSync);
  }

  /**
   * Excludes the deleted worker from the party of the synchronization barrier.
   * @param workerId an id of worker
   */
  synchronized void onWorkerDeleted(final String workerId) {
    // when deleted worker already has sent a sync msg
    if (blockedWorkerIds.contains(workerId)) {
      numWorkersToSync--;
      blockedWorkerIds.remove(workerId);

    // when deleted worker did not send a sync msg yet
    } else {
      numWorkersToSync--;
      tryReleaseWorkers();
    }
  }

  /**
   * Progress to the next state.
   * @param stateMachine a state machine
   */
  static void transitState(final StateMachine stateMachine) {
    final String currentState = stateMachine.getCurrentState();

    switch (currentState) {
    case STATE_INIT:
      stateMachine.setState(STATE_RUN);
      LOG.fine("State transition: STATE_INIT -> STATE_RUN");
      break;
    case STATE_RUN:
      stateMachine.setState(STATE_CLEANUP);
      LOG.fine("State transition: STATE_RUN -> STATE_CLEANUP");
      break;
    case STATE_CLEANUP:
      throw new RuntimeException("No more transition is allowed after STATE_CLEANUP state");
    default:
      throw new RuntimeException("Invalid state");
    }
  }

  /**
   * Wait until all the worker tasks are initialized.
   * @throws InterruptedException
   */
  void waitInitialization() throws InterruptedException {
    initLatch.await();
  }

  /**
   * @return true if all workers are running their main iterations
   */
  boolean areTasksRunning() {
    return globalStateMachine.getCurrentState().equals(STATE_RUN);
  }

  private synchronized void tryReleaseWorkers() {
    if (blockedWorkerIds.size() == numWorkersToSync) {
      LOG.log(Level.INFO, "{0} workers are blocked. Sending response messages to awake them", numWorkersToSync);

      transitState(globalStateMachine);
      // wake threads waiting initialization in waitInitialization()
      if (globalStateMachine.getCurrentState().equals(STATE_RUN)) {
        initLatch.countDown();
      }

      // broadcast responses to blocked workers
      for (final String workerId : blockedWorkerIds) {
        sendResponseMessage(workerId, EMPTY_DATA);
      }

      blockedWorkerIds.clear();
    }
  }

  private void sendResponseMessage(final String workerId, final byte[] data) {
    try {
      aggregationMaster.send(AGGREGATION_CLIENT_NAME, workerId, data);
    } catch (final NetworkException e) {
      LOG.log(Level.INFO, "Target worker has been removed.", e);
    }
  }

  private synchronized void blockWorker(final String workerId) {

    blockedWorkerIds.add(workerId);
    LOG.log(Level.FINE, "Receive a synchronization message from {0}. {1} messages have been received out of {2}.",
        new Object[]{workerId, blockedWorkerIds.size(), numWorkersToSync});

    tryReleaseWorkers();
  }

  final class MessageHandler implements EventHandler<AggregationMessage> {

    @Override
    public void onNext(final AggregationMessage aggregationMessage) {
      final String workerId = aggregationMessage.getSourceId().toString();
      final String localState = codec.decode(aggregationMessage.getData().array());
      final String globalState = globalStateMachine.getCurrentState();

      // In case when a worker's local state is behind the globally synchronized state,
      // this implies the worker is added by EM.
      // If so, the worker is replied to continue until it reaches the global state.
      switch (globalState) {
      case STATE_INIT:
        if (!localState.equals(STATE_INIT)) {
          throw new RuntimeException("Individual workers cannot overtake the global state");
        }
        break;
      case STATE_RUN:
        if (localState.equals(STATE_INIT)) { // let added evaluators skip the initial barriers
          sendResponseMessage(workerId, EMPTY_DATA);
          return;
        } else if (!localState.equals(STATE_RUN)) {
          throw new RuntimeException("Individual workers cannot overtake the global state");
        }
        break;
      case STATE_CLEANUP:
        throw new RuntimeException("Workers never call the global barrier in STATE_CLEANUP state");
      default:
        throw new RuntimeException("Invalid state");
      }

      blockWorker(workerId);
    }
  }
}
