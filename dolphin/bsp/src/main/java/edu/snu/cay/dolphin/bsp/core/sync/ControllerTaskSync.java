/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.bsp.core.sync;

import edu.snu.cay.dolphin.bsp.core.avro.IterationInfo;
import edu.snu.cay.dolphin.bsp.core.sync.avro.AvroSyncMessage;
import edu.snu.cay.dolphin.bsp.core.sync.avro.PauseResult;
import edu.snu.cay.dolphin.bsp.core.sync.avro.Type;
import edu.snu.cay.utils.SingleMessageExtractor;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the Controller Task side of the synchronization protocol.
 * The Controller Task must call update() before each iteration.
 *
 * The following state transitions occur on a successful driver execution:
 * 1. Receive pause request from Driver (-> PAUSE_PENDING)
 * 2. update() is called
 * 3. Send pause result to Driver (-> PAUSED)
 * 4. Receive a resume request from Driver (-> RUNNING)
 * 5. update() returns with true
 */
public final class ControllerTaskSync implements EventHandler<Message<AvroSyncMessage>> {
  private static final Logger LOG = Logger.getLogger(ControllerTaskSync.class.getName());

  private final StateMachine stateMachine;
  private final InjectionFuture<SyncNetworkSetup> pauseNetworkSetup;
  private final IdentifierFactory identifierFactory;
  private final String driverId;

  @Inject
  private ControllerTaskSync(final InjectionFuture<SyncNetworkSetup> pauseNetworkSetup,
                             final IdentifierFactory identifierFactory,
                             @Parameter(DriverIdentifier.class) final String driverId) {
    this.stateMachine = initStateMachine();
    this.pauseNetworkSetup = pauseNetworkSetup;
    this.identifierFactory = identifierFactory;
    this.driverId = driverId;
  }

  private StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(State.RUNNING, "The task is running with no outstanding pause requests")
        .addState(State.PAUSE_PENDING, "There is an outstanding pause request")
        .addState(State.PAUSED, "The task has been paused")
        .setInitialState(State.RUNNING)
        .addTransition(State.RUNNING, State.PAUSE_PENDING,
            "A pause request has been received")
        .addTransition(State.PAUSE_PENDING, State.PAUSED,
            "The task is pausing before an iteration")
        .addTransition(State.PAUSE_PENDING, State.RUNNING,
            "The task has been resumed before it was paused")
        .addTransition(State.PAUSED, State.RUNNING,
            "The paused task has been resumed")
        .build();
  }

  private enum State {
    RUNNING,
    PAUSE_PENDING,
    PAUSED
  }

  /**
   * Checks and waits for a synchronized Driver execution to complete before the given iteration.
   * The return value should be checked to run Task-side operations that are necessary after Driver execution.
   * @param iterationInfo the current iteration, to report to the Driver
   * @return false if Driver did not execute, true if Driver has executed
   * @throws InterruptedException
   */
  public synchronized boolean update(final IterationInfo iterationInfo) throws InterruptedException {
    LOG.entering(ControllerTaskSync.class.getSimpleName(), "update", iterationInfo);

    if (!State.PAUSE_PENDING.equals(stateMachine.getCurrentState())) {
      return false;
    }

    sendPauseResult(iterationInfo);

    LOG.log(Level.INFO, "Pause start.");
    stateMachine.setState(State.PAUSED);
    wait();
    LOG.log(Level.INFO, "Pause end.");

    LOG.exiting(ControllerTaskSync.class.getSimpleName(), "update", iterationInfo);
    return true;
  }

  private void sendPauseResult(final IterationInfo iterationInfo) {
    final PauseResult pauseResult = PauseResult.newBuilder()
        .setIterationInfo(iterationInfo)
        .build();
    final AvroSyncMessage msg = AvroSyncMessage.newBuilder()
        .setType(Type.PauseResult)
        .setPauseResult(pauseResult)
        .build();
    send(msg);
  }

  private void send(final AvroSyncMessage msg) {
    final Connection<AvroSyncMessage> conn = pauseNetworkSetup.get().getConnectionFactory()
        .newConnection(identifierFactory.getNewInstance(driverId));
    try {
      conn.open();
      conn.write(msg);
    } catch (final NetworkException ex) {
      throw new RuntimeException("NetworkException", ex);
    }
  }

  /**
   * Receive pause request and resume request from Driver.
   * @param msg from Driver.
   */
  @Override
  public synchronized void onNext(final Message<AvroSyncMessage> msg) {
    LOG.entering(ControllerTaskSync.class.getSimpleName(), "onNext", msg);

    final AvroSyncMessage innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case PauseRequest:
      onPauseRequestMsg(innerMsg);
      break;
    case ResumeRequest:
      onResumeRequestMsg(innerMsg);
      break;
    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }

    LOG.exiting(ControllerTaskSync.class.getSimpleName(), "onNext", msg);
  }

  private void onPauseRequestMsg(final AvroSyncMessage msg) {
    stateMachine.setState(State.PAUSE_PENDING);
  }

  private void onResumeRequestMsg(final AvroSyncMessage msg) {
    stateMachine.setState(State.RUNNING);
    notify();
  }
}
