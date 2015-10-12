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
package edu.snu.cay.dolphin.core.sync;

import edu.snu.cay.dolphin.core.avro.IterationInfo;
import edu.snu.cay.dolphin.core.sync.avro.AvroSyncMessage;
import edu.snu.cay.dolphin.core.sync.avro.PauseRequest;
import edu.snu.cay.dolphin.core.sync.avro.ResumeRequest;
import edu.snu.cay.dolphin.core.sync.avro.Type;
import edu.snu.cay.utils.SingleMessageExtractor;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the Driver side of the synchronization protocol.
 * The Driver must call onStageStart() and onStageStop() on stage boundaries.
 * Driver execution handler is called only when the Controller Task is successfully paused;
 * the pause failed handler is called if the Controller Task cannot be paused.
 *
 * The following state transitions occur on a successful driver execution:
 * 1. execute(handler, failedHandler) is called
 * 2. Send pause request to ControllerTask (-> WAITING_PAUSE_RESULT)
 * 3. Receive pause result from ControllerTask (-> PAUSED)
 * 4. Run the handler function
 * 5. Send resume request to ControllerTask (-> RUNNING)
 * 6. execute() returns
 */
public final class DriverSync implements EventHandler<Message<AvroSyncMessage>> {
  private static final Logger LOG = Logger.getLogger(DriverSync.class.getName());

  static final String RUNNING = "RUNNING";
  static final String WAITING_PAUSE_RESULT = "WAITING_PAUSE_RESULT";
  static final String PAUSED = "PAUSED";
  static final String CANCELLING = "CANCELLING";

  private final StateMachine stateMachine;
  private final InjectionFuture<SyncNetworkSetup> pauseNetworkSetup;
  private final IdentifierFactory identifierFactory;

  private String groupName;
  private String controllerTaskId;

  private IterationInfo iterationInfo;

  @Inject
  private DriverSync(final InjectionFuture<SyncNetworkSetup> pauseNetworkSetup,
                     final IdentifierFactory identifierFactory) {
    this.pauseNetworkSetup = pauseNetworkSetup;
    this.identifierFactory = identifierFactory;
    this.stateMachine = initStateMachine();
  }

  private StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(RUNNING, "Running normally, with no outstanding pauses")
        .addState(WAITING_PAUSE_RESULT, "Waiting for pause confirmation from Controller Task")
        .addState(PAUSED, "Controller Task has paused")
        .addState(CANCELLING, "Pause is being cancelled")
        .setInitialState(RUNNING)
        // Normal-case transitions
        .addTransition(RUNNING, RUNNING,
            "Running normally")
        .addTransition(RUNNING, WAITING_PAUSE_RESULT,
            "The pause request has been sent")
        .addTransition(WAITING_PAUSE_RESULT, PAUSED,
            "The Controller Task has confirmed pause")
        .addTransition(PAUSED, RUNNING,
            "The resume request has been sent")
        // Error-case transitions
        .addTransition(WAITING_PAUSE_RESULT, CANCELLING,
            "The pause request cannot be satisfied")
        .addTransition(CANCELLING, RUNNING,
            "The pause has been cancelled")
        .build();
  }

  public synchronized void onStageStart(final String newGroupName, final String newControllerTaskId) {
    LOG.log(Level.INFO, "Stage {0} started", newGroupName);
    this.groupName = newGroupName;
    this.controllerTaskId = newControllerTaskId;
    this.iterationInfo = null;
  }

  public synchronized void onStageStop(final String stoppedGroupName) {
    LOG.log(Level.INFO, "Stage {0} stopped", stoppedGroupName);
    if (stoppedGroupName.equals(groupName) &&
        WAITING_PAUSE_RESULT.equals(stateMachine.getCurrentState())) {
      stateMachine.setState(CANCELLING);
      notify();
    }
  }

  /**
   * Execute the handler while the Controller Task waits (or is "paused") before an iteration.
   * Driver execution handler is called only when the Controller Task is successfully paused;
   * the pause failed handler is called if the Controller Task cannot be paused.
   * @param handler the handler to execute when the Controller Task is paused
   * @param pauseFailedHandler the cleanup handler to execute when pause is unsuccessful
   * @throws InterruptedException
   */
  public synchronized void execute(final EventHandler<IterationInfo> handler,
                                   final EventHandler<Object> pauseFailedHandler) throws InterruptedException {
    LOG.entering(DriverSync.class.getSimpleName(), "execute", handler);

    try {
      sendPauseRequest();
    } catch (final NetworkException e) {
      LOG.log(Level.INFO, "Pause was not sent because of a NetworkException", e);
      pauseFailedHandler.onNext(null);
      return;
    }

    stateMachine.setState(WAITING_PAUSE_RESULT);

    wait();
    if (CANCELLING.equals(stateMachine.getCurrentState())) {
      LOG.log(Level.INFO, "Pause was cancelled");
      pauseFailedHandler.onNext(null);
      sendResumeRequest();
      stateMachine.setState(RUNNING);
      return;
    }
    LOG.log(Level.INFO, "Paused at iteration {0}.", iterationInfo);
    stateMachine.setState(PAUSED);

    handler.onNext(iterationInfo);
    sendResumeRequest();
    stateMachine.setState(RUNNING);

    LOG.exiting(DriverSync.class.getSimpleName(), "execute", handler);
  }

  private void sendPauseRequest() throws NetworkException {
    final PauseRequest pauseRequest = PauseRequest.newBuilder()
        .build();
    final AvroSyncMessage msg = AvroSyncMessage.newBuilder()
        .setType(Type.PauseRequest)
        .setPauseRequest(pauseRequest)
        .build();
    send(msg);
  }

  private void sendResumeRequest() {
    final ResumeRequest resumeRequest = ResumeRequest.newBuilder()
        .setIterationInfo(iterationInfo)
        .build();
    final AvroSyncMessage msg = AvroSyncMessage.newBuilder()
        .setType(Type.ResumeRequest)
        .setResumeRequest(resumeRequest)
        .build();
    try {
      send(msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("Connection to the paused ControllerTask failed", e);
    }
  }

  private void send(final AvroSyncMessage msg) throws NetworkException {
    final Connection<AvroSyncMessage> conn = pauseNetworkSetup.get().getConnectionFactory()
        .newConnection(identifierFactory.getNewInstance(controllerTaskId));
    conn.open(); // TODO #211: It may be important to avoid a costly timeout here when the task has since completed.
    conn.write(msg);
  }

  /**
   * Receive pause result from ControllerTask.
   * @param msg from ControllerTask
   */
  @Override
  public synchronized void onNext(final Message<AvroSyncMessage> msg) {
    LOG.entering(DriverSync.class.getSimpleName(), "onNext", msg);

    final AvroSyncMessage innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case PauseResult:
      onPauseResultMsg(innerMsg);
      break;
    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }

    LOG.exiting(DriverSync.class.getSimpleName(), "onNext", msg);
  }

  private void onPauseResultMsg(final AvroSyncMessage msg) {
    iterationInfo = msg.getPauseResult().getIterationInfo();
    assertStage(iterationInfo);
    notify();
  }

  private void assertStage(final IterationInfo pausedIteration) {
    if (!WAITING_PAUSE_RESULT.equals(stateMachine.getCurrentState())) {
      throw new RuntimeException("Unexpected state on pause result receipt " + stateMachine.getCurrentState());
    } else if (!pausedIteration.getCommGroupName().toString().equals(groupName)) {
      throw new RuntimeException("Expected groupName " + groupName +
          " but received " + pausedIteration.getCommGroupName());
    }
  }
}
