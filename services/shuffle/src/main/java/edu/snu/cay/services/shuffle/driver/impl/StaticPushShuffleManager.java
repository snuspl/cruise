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
package edu.snu.cay.services.shuffle.driver.impl;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.driver.DSControlMessageSender;
import edu.snu.cay.services.shuffle.driver.ShuffleManager;
import edu.snu.cay.services.shuffle.evaluator.impl.StaticPushShuffle;
import edu.snu.cay.services.shuffle.evaluator.operator.impl.PushShuffleReceiverImpl;
import edu.snu.cay.services.shuffle.evaluator.operator.impl.PushShuffleReceiverState;
import edu.snu.cay.services.shuffle.evaluator.operator.impl.PushShuffleSenderImpl;
import edu.snu.cay.services.shuffle.network.ControlMessageNetworkSetup;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.services.shuffle.driver.ShuffleConfigurationSerializer;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.Configuration;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ShuffleManager implementation for static push-based shuffle.
 *
 * The initial shuffle description can never be changed. Users cannot add or remove more tasks
 * to the shuffle and cannot change the key, value codecs and shuffle strategy after the manager is created.
 */
@DriverSide
public final class StaticPushShuffleManager implements ShuffleManager {

  private static final Logger LOG = Logger.getLogger(StaticPushShuffleManager.class.getName());

  private final ShuffleDescription shuffleDescription;
  private final ShuffleConfigurationSerializer shuffleConfSerializer;
  private final ControlMessageNetworkSetup controlMessageNetworkSetup;
  private final DSControlMessageSender controlMessageSender;

  private final StateManager stateManager;

  private PushShuffleListener pushShuffleListener;

  @Inject
  private StaticPushShuffleManager(
      final ShuffleDescription shuffleDescription,
      final ShuffleConfigurationSerializer shuffleConfSerializer,
      final ControlMessageNetworkSetup controlMessageNetworkSetup,
      final DSControlMessageSender controlMessageSender) {

    controlMessageNetworkSetup.setControlMessageHandlerAndLinkListener(
        new ControlMessageHandler(), new ControlLinkListener());
    this.shuffleDescription = shuffleDescription;
    this.shuffleConfSerializer = shuffleConfSerializer;
    this.controlMessageNetworkSetup = controlMessageNetworkSetup;
    this.controlMessageSender = controlMessageSender;
    this.stateManager = new StateManager();
  }

  /**
   * @param endPointId end point id
   * @return Serialized shuffle description for the endPointId
   */
  @Override
  public Configuration getShuffleConfiguration(final String endPointId) {
    return shuffleConfSerializer.serialize(
        StaticPushShuffle.class,
        PushShuffleSenderImpl.class,
        PushShuffleReceiverImpl.class,
        shuffleDescription,
        endPointId
    );
  }

  /**
   * @return the initial shuffle description
   */
  @Override
  public ShuffleDescription getShuffleDescription() {
    return shuffleDescription;
  }

  /**
   * Close the network setup.
   */
  @Override
  public void close() {
    controlMessageNetworkSetup.close();
  }

  /**
   * Shutdown the shuffle manager. All senders and receivers are shutdown after the ongoing iteration is finished.
   */
  public void shutdown() {
    stateManager.shutdown();
  }

  private final class ControlMessageHandler implements EventHandler<Message<ShuffleControlMessage>> {

    @Override
    public void onNext(final Message<ShuffleControlMessage> message) {
      final ShuffleControlMessage controlMessage = message.getData().iterator().next();
      final String endPointId = message.getSrcId().toString();
      switch (controlMessage.getCode()) {
      case PushShuffleCode.SENDER_INITIALIZED:
        stateManager.onSenderInitialized(endPointId);
        break;
      case PushShuffleCode.SENDER_FINISHED:
        stateManager.onSenderFinished(endPointId);
        break;
      case PushShuffleCode.RECEIVER_INITIALIZED:
        stateManager.onReceiverInitialized(endPointId);
        break;
      case PushShuffleCode.RECEIVER_COMPLETED:
        // TODO #278: Get the number of received tuples without using string
        final int numReceivedTuples = Integer.parseInt(controlMessage.get(0));
        stateManager.onReceiverCompleted(endPointId, numReceivedTuples);
        break;
      case PushShuffleCode.RECEIVER_READIED:
        stateManager.onReceiverReadied(endPointId);
        break;
      case PushShuffleCode.RECEIVER_FINISHED:
        stateManager.onReceiverFinished(endPointId);
        break;
      default:
        throw new IllegalStateException("Unknown code " + controlMessage.getCode() + " from " + endPointId);
      }
    }
  }

  public synchronized void setPushShuffleListener(final PushShuffleListener pushShuffleListener) {
    this.pushShuffleListener = pushShuffleListener;
  }

  private final class ControlLinkListener implements LinkListener<Message<ShuffleControlMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleControlMessage> message) {
      LOG.log(Level.FINE, "A ShuffleControlMessage was successfully sent : {0}", message);
    }

    @Override
    public void onException(
        final Throwable cause,
        final SocketAddress socketAddress,
        final Message<ShuffleControlMessage> message) {
      LOG.log(Level.WARNING, "An exception occurred while sending a ShuffleControlMessage. cause : {0}," +
          " socket address : {1}, message : {2}", new Object[]{cause, socketAddress, message});
      // TODO #67: failure handling.
      throw new RuntimeException("An exception occurred while sending a ShuffleControlMessage", cause);
    }
  }

  /**
   * State manager for StaticPushShuffleManager. This receives control messages
   * from senders and receivers, and makes a proper state transition as communicating
   * with senders and receivers.
   */
  private final class StateManager {

    private static final String INIT = "INIT";
    private static final String CAN_SEND = "CAN_SEND";
    private static final String RECEIVERS_COMPLETED = "RECEIVERS_COMPLETED";
    private static final String FINISHED = "FINISHED";

    private boolean initialized;
    private boolean shutdown;
    private final int totalNumSenders;
    private final int totalNumReceivers;
    private Map<String, StateMachine> receiverStateMachineMap;
    private final List<String> initializedSenderIdList;
    private int numInitializedReceivers;
    private int numReadiedReceivers;
    private int numCompletedReceivers;
    private int numFinishedSenders;
    private int numFinishedReceivers;
    private int numCompletedIterations;
    private int numReceivedTuplesInIteration;
    private long iterationStartTime;

    private StateMachine stateMachine;

    private StateManager() {
      this.initializedSenderIdList = new ArrayList<>();
      this.totalNumSenders = shuffleDescription.getSenderIdList().size();
      this.totalNumReceivers = shuffleDescription.getReceiverIdList().size();
      this.receiverStateMachineMap = new HashMap<>(totalNumReceivers);

      for (final String receiverId : shuffleDescription.getReceiverIdList()) {
        receiverStateMachineMap.put(receiverId, PushShuffleReceiverState.createStateMachine());
      }

      this.stateMachine = StateMachine.newBuilder()
          .addState(INIT, "Wait for all receivers are initialized")
          .addState(CAN_SEND, "Prepared senders can send data to receivers")
          .addState(RECEIVERS_COMPLETED, "All receivers were completed to receive data from senders in an iteration")
          .addState(FINISHED, "All senders and receivers were finished")
          .setInitialState(INIT)
          .addTransition(INIT, CAN_SEND,
              "When RECEIVER_INITIALIZED messages arrived from all receivers."
                  + " It sends SENDER_CAN_SEND messages to all initialized senders.")
          .addTransition(CAN_SEND, RECEIVERS_COMPLETED,
              "When RECEIVER_COMPLETED messages arrived from all receivers." +
                  " It broadcasts ALL_RECEIVERS_COMPLETED messages to all receivers.")
          .addTransition(RECEIVERS_COMPLETED, CAN_SEND,
              "When RECEIVER_READIED messages arrived from all receivers"
                  + " It broadcasts SENDER_CAN_SEND messages to all senders.")
          .addTransition(RECEIVERS_COMPLETED, FINISHED,
              "If shutdown() method was called before.")
          .build();
    }

    private synchronized void shutdown() {
      shutdown = true;
    }

    private void sendSenderCanSendMessage(final String senderId) {
      try {
        controlMessageSender.send(senderId, PushShuffleCode.SENDER_CAN_SEND);
      } catch (final NetworkException e) {
        // cannot open connection to sender
        // TODO #67: failure handling.
        throw new RuntimeException(e);
      }
    }

    private synchronized void onSenderInitialized(final String senderId) {
      if (initialized) {
        sendSenderCanSendMessage(senderId);
      } else {
        initializedSenderIdList.add(senderId);
      }
    }

    private synchronized void onSenderFinished(final String senderId) {
      stateMachine.checkState(RECEIVERS_COMPLETED);
      numFinishedSenders++;

      if (isAllSendersAndReceiversFinished()) {
        finishManager();
      }
    }

    private synchronized void onReceiverInitialized(final String receiverId) {
      stateMachine.checkState(INIT);
      receiverStateMachineMap.get(receiverId).checkState(PushShuffleReceiverState.RECEIVING);
      LOG.log(Level.FINE, "A receiver is initialized " + receiverId);
      numInitializedReceivers++;
      broadcastIfAllReceiversAreInitialized();
    }

    private void broadcastIfAllReceiversAreInitialized() {
      if (numInitializedReceivers == totalNumReceivers) {
        initialized = true;
        LOG.log(Level.INFO, "All receivers are initialized");
        if (stateMachine.compareAndSetState(INIT, CAN_SEND)) {
          throw new RuntimeException("The expected current state [" + INIT +
              "] is different from the actual state");
        }
        for (final String senderId : initializedSenderIdList) {
          sendSenderCanSendMessage(senderId);
        }
        iterationStartTime = System.currentTimeMillis();
      }
    }

    private synchronized void onReceiverCompleted(final String receiverId, final int numReceivedTuples) {
      stateMachine.checkState(CAN_SEND);
      if (receiverStateMachineMap.get(receiverId)
          .compareAndSetState(PushShuffleReceiverState.RECEIVING, PushShuffleReceiverState.COMPLETED)) {
        throw new RuntimeException("The expected current state [" + PushShuffleReceiverState.RECEIVING +
            "] is different from the actual state");
      }

      numReceivedTuplesInIteration += numReceivedTuples;
      numCompletedReceivers++;

      LOG.log(Level.FINE, "A receiver " + receiverId + " was completed to receive data.");

      if (numCompletedReceivers == totalNumReceivers) {
        numCompletedReceivers = 0;
        if (stateMachine.compareAndSetState(CAN_SEND, RECEIVERS_COMPLETED)) {
          throw new RuntimeException("The expected current state [" + CAN_SEND +
              "] is different from the actual state");
        }
        LOG.log(Level.INFO, "All receivers were completed to receive data.");
        final long prevStartTime = iterationStartTime;
        iterationStartTime = System.currentTimeMillis();
        final IterationInfo info = new IterationInfo(++numCompletedIterations, numReceivedTuplesInIteration,
            iterationStartTime - prevStartTime);
        numReceivedTuplesInIteration = 0;
        pushShuffleListener.onIterationCompleted(info);
        if (shutdown) {
          shutdownAllSendersAndReceivers();
        } else {
          broadcastToReceivers(PushShuffleCode.RECEIVER_CAN_RECEIVE);
        }
      }
    }

    private void shutdownAllSendersAndReceivers() {
      broadcastToReceivers(PushShuffleCode.RECEIVER_SHUTDOWN);
      broadcastToSenders(PushShuffleCode.SENDER_SHUTDOWN);
    }

    private synchronized void onReceiverReadied(final String receiverId) {
      if (receiverStateMachineMap.get(receiverId)
          .compareAndSetState(PushShuffleReceiverState.COMPLETED, PushShuffleReceiverState.RECEIVING)) {
        throw new RuntimeException("The expected current state [" + PushShuffleReceiverState.COMPLETED +
            "] is different from the actual state");
      }
      numReadiedReceivers++;

      LOG.log(Level.FINE, "A receiver " + receiverId + " is ready to receive data.");

      if (numReadiedReceivers == totalNumReceivers) {
        numReadiedReceivers = 0;
        LOG.log(Level.INFO, "Broadcast to all senders that they can send data to receivers.");
        if (stateMachine.compareAndSetState(RECEIVERS_COMPLETED, CAN_SEND)) {
          throw new RuntimeException("The expected current state [" + RECEIVERS_COMPLETED +
              "] is different from the actual state");
        }
        broadcastToSenders(PushShuffleCode.SENDER_CAN_SEND);
      }
    }

    private synchronized void onReceiverFinished(final String receiverId) {
      stateMachine.checkState(RECEIVERS_COMPLETED);
      if (receiverStateMachineMap.get(receiverId)
          .compareAndSetState(PushShuffleReceiverState.COMPLETED, PushShuffleReceiverState.FINISHED)) {
        throw new RuntimeException("The expected current state [" + PushShuffleReceiverState.COMPLETED +
            "] is different from the actual state");
      }
      numFinishedReceivers++;

      if (isAllSendersAndReceiversFinished()) {
        finishManager();
      }
    }

    private boolean isAllSendersAndReceiversFinished() {
      return numFinishedSenders == totalNumSenders && numFinishedReceivers == totalNumReceivers;
    }

    private void finishManager() {
      LOG.log(Level.INFO, "The StaticPushShuffleManager is finished");
      if (stateMachine.compareAndSetState(RECEIVERS_COMPLETED, FINISHED)) {
        throw new RuntimeException("The expected current state [" + RECEIVERS_COMPLETED +
            "] is different from the actual state");
      }
      pushShuffleListener.onFinished();
    }

    private void broadcastToSenders(final int code) {
      for (final String senderId : shuffleDescription.getSenderIdList()) {
        try {
          controlMessageSender.send(senderId, code);
        } catch (final NetworkException e) {
          // cannot open connection to sender
          // TODO #67: failure handling.
          throw new RuntimeException(e);
        }
      }
    }

    private void broadcastToReceivers(final int code) {
      for (final String receiverId : shuffleDescription.getReceiverIdList()) {
        try {
          controlMessageSender.send(receiverId, code);
        } catch (final NetworkException e) {
          // cannot open connection to receiver
          // TODO #67: failure handling.
          throw new RuntimeException(e);
        }
      }
    }
  }
}
