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
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
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

  /**
   * A latch that releases waiting threads when workers finish initialization.
   */
  private final CountDownLatch initLatch = new CountDownLatch(1);

  /**
   * A boolean flag that becomes true when at least one worker finishes its main iterations
   * and is waiting for response to progress to the cleanup state.
   */
  private AtomicBoolean waitingCleanup = new AtomicBoolean(false);

  /**
   * A boolean flag that becomes true when driver allows SyncManager to let workers enter the cleanup state.
   */
  private boolean allowCleanup = false;

  private final AggregationMaster aggregationMaster;

  private final Codec<Enum> codec;

  @GuardedBy("this")
  private final StateMachine globalStateMachine;

  /**
   * The total number of workers to sync.
   */
  @GuardedBy("this")
  private int numWorkers = 0;

  /**
   * The total number of initial workers participating in job.
   */
  private final int numInitialWorkers;

  /**
   * A set that maintains workers that have sent a sync msg for the current barrier.
   */
  @GuardedBy("this")
  private final Set<String> blockedWorkerIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Inject
  private SynchronizationManager(final AggregationMaster aggregationMaster,
                                 final SerializableCodec<Enum> codec,
                                 final DataLoadingService dataLoadingService) {
    this.aggregationMaster = aggregationMaster;
    this.codec = codec;

    // TODO #452: Decouple numWorkers from data input splits
    this.numInitialWorkers = dataLoadingService.getNumberOfPartitions();
    this.globalStateMachine = initStateMachine();
  }

  static StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(State.INIT, "Workers are initializing themselves")
        .addState(State.RUN, "Workers are running their tasks")
        .addState(State.CLEANUP, "Workers are cleaning up the task")
        .addTransition(State.INIT, State.RUN, "The initialization is finished, time to start running task")
        .addTransition(State.RUN, State.CLEANUP, "The task execution is finished, time to clean up the task")
        .setInitialState(State.INIT)
        .build();
  }

  public enum State {
    INIT,
    RUN,
    CLEANUP
  }

  /**
   * Embraces the newly added worker to be synchronized from now.
   */
  synchronized void onWorkerAdded() {
    // increase the number of workers to sync
    numWorkers++;
    LOG.log(Level.INFO, "Total number of workers participating in the synchronization = {0}", numWorkers);
  }

  /**
   * Excludes the deleted worker from the party of the synchronization barrier.
   * @param workerId an id of worker
   */
  synchronized void onWorkerDeleted(final String workerId) {
    numWorkers--;
    LOG.log(Level.INFO, "Total number of workers participating in the synchronization = {0}", numWorkers);
    // when deleted worker already has sent a sync msg
    if (blockedWorkerIds.contains(workerId)) {
      blockedWorkerIds.remove(workerId);

    // when deleted worker did not send a sync msg yet
    } else {
      // should be done asynchronously
      Executors.newSingleThreadExecutor().execute(this::tryReleaseWorkers);
    }
  }

  /**
   * Progress to the next state.
   * @param stateMachine a state machine
   */
  static void transitState(final StateMachine stateMachine) {
    final State currentState = (State) stateMachine.getCurrentState();

    switch (currentState) {
    case INIT:
      stateMachine.setState(State.RUN);
      LOG.log(Level.INFO, "State transition: STATE_INIT -> STATE_RUN");
      break;
    case RUN:
      stateMachine.setState(State.CLEANUP);
      LOG.log(Level.INFO, "State transition: STATE_RUN -> STATE_CLEANUP");
      break;
    case CLEANUP:
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
   * @return true if workers are in the initialization state
   */
  synchronized boolean workersInitializing() {
    return globalStateMachine.getCurrentState().equals(State.INIT);
  }

  /**
   * Allow sending response to workers to start the cleanup stage.
   */
  synchronized void allowWorkersCleanup() {
    allowCleanup = true;
    notifyAll();
  }

  /**
   * @return true if at least one worker is waiting for response to progress to the cleanup state
   */
  boolean waitingCleanup() {
    return waitingCleanup.get();
  }

  private synchronized void tryReleaseWorkers() {
    final State currentState = (State) globalStateMachine.getCurrentState();

    // check whether all initial workers are added through onWorkerAdded()
    if (currentState.equals(State.INIT)) {
      if (numWorkers < numInitialWorkers) {
        LOG.log(Level.FINE, "Need {0} more initial workers to start", numInitialWorkers - numWorkers);
        return;
      }
    }

    if (blockedWorkerIds.size() == numWorkers) {
      LOG.log(Level.INFO, "Try releasing {0} blocked workers: {1}", new Object[]{numWorkers, blockedWorkerIds});

      // wake threads waiting initialization in waitInitialization()
      if (currentState.equals(State.INIT)) {
        initLatch.countDown();

      // Let workers enter the cleanup state after assuring that there's no ongoing optimization.
      // Note that in the STATE_CLEANUP state, orchestrator never trigger further optimization.
      } else if (currentState.equals(State.RUN)) {
        final int numWorkersBeforeSleep = numWorkers;
        LOG.log(Level.INFO, "Wait for driver to allow workers to enter cleanup state");
        while (!allowCleanup) {
          try {
            wait();
          } catch (final InterruptedException e) {
            LOG.log(Level.WARNING, "Interrupted while waiting for the optimization to be done", e);
          }
        }

        // numWorkers may have changed while waiting for allowCleanup, by the last reconfiguration plan.
        // We should handle the case differently if new workers have been added.
        // Here we assume that optimizers never generate a plan that includes both Add and Del of workers.
        final int numAddedWorkers = numWorkers - numWorkersBeforeSleep;
        if (numAddedWorkers > 0) {

          LOG.log(Level.FINE, "{0} more workers were added while waiting for driver to allow cleanup",
              numAddedWorkers);

          if (blockedWorkerIds.size() < numWorkers) {
            LOG.log(Level.INFO, "Cancel releasing workers" +
                    " because we have {0} more workers that have not requested their cleanup barrier.",
                numWorkers - blockedWorkerIds.size());

            // Cancel releasing workers.
            // Added workers will invoke this method again when they are going to enter cleanup state.
            return;
          }
        }
      }

      transitState(globalStateMachine);

      LOG.log(Level.INFO, "Send response message to {0} blocked workers: {1}",
          new Object[]{numWorkers, blockedWorkerIds});
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
    LOG.log(Level.INFO, "Receive a synchronization message from {0}. {1} messages have been received out of {2}.",
        new Object[]{workerId, blockedWorkerIds.size(), numWorkers});

    tryReleaseWorkers();
  }

  final class MessageHandler implements EventHandler<AggregationMessage> {

    @Override
    public void onNext(final AggregationMessage aggregationMessage) {
      synchronized (SynchronizationManager.this) {
        final String workerId = aggregationMessage.getSourceId().toString();
        final State localState = (State) codec.decode(aggregationMessage.getData().array());
        final State globalState = (State) globalStateMachine.getCurrentState();

        // In case when a worker's local state is behind the globally synchronized state,
        // this implies the worker is added by EM.
        // If so, the worker is replied to continue until it reaches the global state.
        switch (globalState) {
        case INIT:
          if (localState.equals(State.INIT)) {
            blockWorker(workerId);
          } else {
            throw new RuntimeException("Individual workers cannot overtake the global state");
          }

          break;
        case RUN:
          if (localState.equals(State.RUN)) {
            // worker finishes their main iteration and is waiting for response to enter the cleanup stage
            if (waitingCleanup.compareAndSet(false, true)) {
              LOG.log(Level.INFO, "One of worker starts waiting for cleanup");
            }
            blockWorker(workerId);
          } else if (localState.equals(State.INIT)) {
            // let added evaluators skip the initial barriers
            sendResponseMessage(workerId, EMPTY_DATA);
          } else {
            throw new RuntimeException("Individual workers cannot overtake the global state");
          }

          break;
        case CLEANUP:
          // Though workers already exit their barrier before CLEANUP phase,
          // added workers can request the barriers. In this case, we let the workers proceed immediately.
          sendResponseMessage(workerId, EMPTY_DATA);
          break;
        default:
          throw new RuntimeException("Invalid state");
        }
      }
    }
  }
}
