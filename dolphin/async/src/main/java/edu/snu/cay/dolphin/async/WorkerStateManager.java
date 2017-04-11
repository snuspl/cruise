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

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.master.MasterSideCentCommMsgSender;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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

  static final String CENT_COMM_CLIENT_NAME = WorkerStateManager.class.getName();

  private final MasterSideCentCommMsgSender masterSideCentCommMsgSender;

  private final Codec<WorkerGlobalBarrier.State> codec;

  /**
   * The total number of workers.
   */
  private final int numWorkers;

  @GuardedBy("this")
  private final StateMachine stateMachine;

  /**
   * A set of ids of workers to be synchronized.
   */
  @GuardedBy("this")
  private final Set<String> runningWorkerIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

  /**
   * A set maintaining worker ids of whom have sent a sync msg for the barrier.
   */
  @GuardedBy("this")
  private final Set<String> blockedWorkerIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Inject
  private WorkerStateManager(final MasterSideCentCommMsgSender masterSideCentCommMsgSender,
                             @Parameter(DolphinParameters.NumWorkers.class) final int numWorkers,
                             final SerializableCodec<WorkerGlobalBarrier.State> codec) {
    this.masterSideCentCommMsgSender = masterSideCentCommMsgSender;
    this.numWorkers = numWorkers;
    LOG.log(Level.INFO, "Initialized with NumWorkers: {0}", numWorkers);
    this.codec = codec;
    this.stateMachine = initStateMachine();
  }

  private enum State {
    INIT,
    RUN,
    OPTIMIZE,
    RUN_FINISHING,
    CLEANUP
  }

  private static StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(State.INIT, "Workers are initializing themselves")
        .addState(State.RUN, "Workers are running their tasks. Optimization can take place")
        .addState(State.OPTIMIZE, "Job are in optimization phase. The number of workers can be changed in this state")
        .addState(State.RUN_FINISHING, "At least one worker has finished RUN phase. From now, optimization is banned.")
        .addState(State.CLEANUP, "Workers are cleaning up the task")
        .addTransition(State.INIT, State.RUN, "The worker init is finished, time to start running task")
        .addTransition(State.RUN, State.OPTIMIZE, "Start optimization.")
        .addTransition(State.OPTIMIZE, State.RUN, "Optimization is finished.")
        .addTransition(State.RUN, State.RUN_FINISHING, "A worker has finished its RUN procedure.")
        .addTransition(State.RUN_FINISHING, State.CLEANUP, "The task execution is finished, time to clean up the task")
        .setInitialState(State.INIT)
        .build();
  }

  /**
   * A latch that will be released when workers enter RUN state.
   */
  private final CountDownLatch runStateLatch = new CountDownLatch(1);

  /**
   * Waits until workers to enter RUN state.
   */
  public void waitWorkersToEnterRunState() {
    while (true) {
      try {
        runStateLatch.await();
        break;
      } catch (InterruptedException e) {
        // ignore and keep waiting
      }
    }
  }

  /**
   * Tries to enter the optimization state.
   * Workers cannot enter CLEANUP state, even all existing workers finish RUN phase.
   * At the first try, it's good to call {@link #waitWorkersToEnterRunState()}.
   * @return True if it succeeds to enter optimization phase
   */
  public synchronized boolean tryEnterOptimization() {
    final State curState = (State) stateMachine.getCurrentState();
    if (!curState.equals(State.RUN)) {
      LOG.log(Level.INFO, "Fail to enter Optimization state. Current state: {0}", curState);
      return false;
    } else {
      stateMachine.setState(State.OPTIMIZE);
      return true;
    }
  }

  /**
   * Should be called when the optimization has been finished.
   * It updates the entry of running/blocked workers as the result of optimization.
   * @param addedWorkers a set of added worker ids
   * @param deletedWorkers a ser of deleted worker ids
   */
  public synchronized void onOptimizationFinished(final Set<String> addedWorkers,
                                                  final Set<String> deletedWorkers) {
    stateMachine.checkState(State.OPTIMIZE);

    LOG.log(Level.INFO, "NumAddedWorkers: {0}, NumDeletedWorkers: {1}",
        new Object[]{addedWorkers.size(), deletedWorkers.size()});
    LOG.log(Level.FINE, "AddedWorkers: {0}, DeletedWorkers: {1}", new Object[]{addedWorkers, deletedWorkers});

    if (!runningWorkerIds.containsAll(deletedWorkers) ||
        !Collections.disjoint(addedWorkers, deletedWorkers) ||
        !Collections.disjoint(addedWorkers, runningWorkerIds)) {
      throw new IllegalStateException("The change by optimization is invalid");
    }

    final int numRunningWorkersBefore = runningWorkerIds.size();

    runningWorkerIds.addAll(addedWorkers);
    runningWorkerIds.removeAll(deletedWorkers);
    blockedWorkerIds.removeAll(deletedWorkers); // workers who has been deleted at the cleanup barrier

    LOG.log(Level.INFO, "The number of running workers changes by optimization. {0} -> {1}",
        new Object[]{numRunningWorkersBefore, runningWorkerIds.size()});

    stateMachine.setState(State.RUN);
    if (!blockedWorkerIds.isEmpty()) {
      stateMachine.setState(State.RUN_FINISHING);
    }

    tryReleasingWorkers();
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
      stateMachine.setState(State.RUN_FINISHING);
      LOG.log(Level.INFO, String.format("State transition: %s -> %s", State.RUN, State.RUN_FINISHING));
      break;
    case RUN_FINISHING:
      stateMachine.setState(State.CLEANUP);
      LOG.log(Level.INFO, String.format("State transition: %s -> %s", State.RUN_FINISHING, State.CLEANUP));
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
    LOG.log(Level.INFO, "Release {0} blocked workers: {1}",
        new Object[]{blockedWorkerIds.size(), blockedWorkerIds});
    // broadcast responses to blocked workers
    for (final String workerId : blockedWorkerIds) {
      sendResponseMessage(workerId);
    }

    blockedWorkerIds.clear();
  }

  private void sendResponseMessage(final String workerId) {
    try {
      masterSideCentCommMsgSender.send(CENT_COMM_CLIENT_NAME, workerId, EMPTY_DATA);
    } catch (final NetworkException e) {
      LOG.log(Level.INFO, String.format("Fail to send msg to worker %s.", workerId), e);
    }
  }

  /**
   * Blocks this worker. It can be released by {@link #releaseWorkers()} later.
   * @param workerId a worker id
   */
  private synchronized void blockWorker(final String workerId) {
    blockedWorkerIds.add(workerId);
    LOG.log(Level.INFO, "Block worker {0}. [{1} / {2}]",
        new Object[]{workerId, blockedWorkerIds.size(), runningWorkerIds.size()});
  }

  private synchronized void tryReleasingWorkers() {
    if (blockedWorkerIds.containsAll(runningWorkerIds)) {
      transitToNextState();
      releaseWorkers();
    }
  }

  /**
   * Handles messages from workers.
   * @param workerId a worker id
   * @param localState the worker's local state
   */
  private synchronized void onWorkerMsg(final String workerId, final WorkerGlobalBarrier.State localState) {
    final State globalState = (State) stateMachine.getCurrentState();

    switch (globalState) {
    case INIT:
      if (localState.equals(WorkerGlobalBarrier.State.INIT)) {
        blockWorker(workerId);

        // collect worker ids until it reaches NumWorkers
        runningWorkerIds.add(workerId);

        LOG.log(Level.INFO, "Worker {0} is initialized. [{1} / {2}]",
            new Object[]{workerId, runningWorkerIds.size(), numWorkers});

        // all worker finishes their initialization and is waiting for response to enter the run stage
        if (runningWorkerIds.size() == numWorkers) {
          transitToNextState();
          releaseWorkers();
        }
      } else {
        throw new RuntimeException(String.format("Worker %s is in invalid state: %s", workerId, localState));
      }
      break;
    case RUN:
      if (localState.equals(WorkerGlobalBarrier.State.RUN)) {
        // the first worker that finishes RUN phase will make it transit to RUN_FINISHING state
        LOG.log(Level.INFO, "One worker finishes RUN stage.");
        transitToNextState();

        blockWorker(workerId);
        tryReleasingWorkers(); // will release if there's only one worker
      } else if (localState.equals(WorkerGlobalBarrier.State.INIT)) {
        // let added workers skip the initial barriers
        sendResponseMessage(workerId);
      } else {
        throw new RuntimeException(String.format("Worker %s is in invalid state: %s", workerId, localState));
      }
      break;
    case RUN_FINISHING:
      if (localState.equals(WorkerGlobalBarrier.State.RUN)) {
        blockWorker(workerId);
        tryReleasingWorkers();
      } else if (localState.equals(WorkerGlobalBarrier.State.INIT)) {
        // let added workers skip the initial barriers
        sendResponseMessage(workerId);
      } else {
        throw new RuntimeException(String.format("Worker %s is in invalid state: %s", workerId, localState));
      }
      break;
    case OPTIMIZE:
      if (localState.equals(WorkerGlobalBarrier.State.RUN)) {
        blockWorker(workerId);
      } else if (localState.equals(WorkerGlobalBarrier.State.INIT)) {
        // let added workers skip the initial barriers
        sendResponseMessage(workerId);
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

  final class MessageHandler implements EventHandler<CentCommMsg> {

    @Override
    public void onNext(final CentCommMsg centCommMsg) {
      final String workerId = centCommMsg.getSourceId().toString();
      final WorkerGlobalBarrier.State localState = codec.decode(centCommMsg.getData().array());

      onWorkerMsg(workerId, localState);
    }
  }
}
