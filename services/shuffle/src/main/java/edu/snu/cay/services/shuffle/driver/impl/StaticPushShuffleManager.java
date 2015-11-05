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
import java.util.*;
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
        stateManager.onReceiverCompleted(endPointId);
        break;
      case PushShuffleCode.RECEIVER_READIED:
        stateManager.onReceiverReadied(endPointId);
        break;
      case PushShuffleCode.RECEIVER_FINISHED:
        stateManager.onReceiverFinished(endPointId);
        break;
      case PushShuffleCode.SENDER_UPDATED:
        stateManager.onSenderUpdated();
        break;
      case PushShuffleCode.RECEIVER_UPDATED:
        stateManager.onReceiverUpdated();
        break;
      default:
        throw new IllegalStateException("Unknown code " + controlMessage.getCode() + " from " + endPointId);
      }
    }
  }

  public synchronized void setPushShuffleListener(final PushShuffleListener pushShuffleListener) {
    this.pushShuffleListener = pushShuffleListener;
  }

  public synchronized void addSender(final String senderId) {
    shuffleDescription.addSender(senderId);
    stateManager.addSender(senderId);
  }

  public synchronized void removeSender(final String senderId) {
    shuffleDescription.removeSender(senderId);
    stateManager.removeSender(senderId);
  }

  public synchronized void addReceiver(final String receiverId) {
    shuffleDescription.addReceiver(receiverId);
    stateManager.addReceiver(receiverId);
  }

  public synchronized void removeReceiver(final String receiverId) {
    shuffleDescription.removeReceiver(receiverId);
    stateManager.removeReceiver(receiverId);
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
    private static final String READY = "READY";

    private static final String ADD_SENDER_PREFIX = "ADDSENDER-";
    private static final String REMOVE_SENDER_PREFIX = "REMOVESENDER-";
    private static final String ADD_RECEIVER_PREFIX = "ADDRECEIVER-";
    private static final String REMOVE_RECEIVER_PREFIX = "REMOVERECEIVER-";

    private boolean initialized;
    private boolean shutdown;
    private int totalNumSenders;
    private int totalNumReceivers;
    private Map<String, StateMachine> receiverStateMachineMap;
    private final List<String> initializedSenderIdList;
    private int numInitializedReceivers;
    private int numReadiedReceivers;
    private int numCompletedReceivers;
    private int numFinishedSenders;
    private int numFinishedReceivers;
    private int numCompletedIterations;
    private boolean isDescriptionUpdated;
    private boolean isFirstIteration;
    private List<String> descriptionUpdateList;

    private List<String> newSenderIdList;
    private List<String> newReceiverIdList;
    private List<String> removeSenderIdList;
    private List<String> removeReceiverIdList;
    private int numUpdatedSenders;
    private int numUpdatedReceivers;
    private int numNewInitializedSenders;
    private int numNewInitializedReceivers;


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
          .addState(READY, "Wait for all senders and receivers are updated")
          .setInitialState(INIT)
          .addTransition(INIT, CAN_SEND,
              "When RECEIVER_INITIALIZED messages arrived from all receivers."
                  + " It sends SENDER_CAN_SEND messages to all initialized senders.")
          .addTransition(CAN_SEND, RECEIVERS_COMPLETED,
              "When RECEIVER_COMPLETED messages arrived from all receivers." +
                  " It broadcasts ALL_RECEIVERS_COMPLETED messages to all receivers.")
          .addTransition(RECEIVERS_COMPLETED, CAN_SEND,
              "When RECEIVER_READIED messages arrived from all receivers and ShuffleDescription is not changed."
                  + " It broadcasts SENDER_CAN_SEND messages to all senders.")
          .addTransition(RECEIVERS_COMPLETED, READY,
              "When RECEIVER_READIED messages arrived from all receivers and ShuffleDescription is changed."
                  + "It broadcasts SENDER_DESCRIPTION_UPDATE messages to all senders and "
                  + "RECEIVER_DESCRIPTION_UPDATE messages to all receivers")
          .addTransition(READY, CAN_SEND,
              "When all senders and receivers are updated and new senders or receivers are initialized."
                  + " It broadcasts SENDER_CAN_SEND messages to all senders.")
          .addTransition(RECEIVERS_COMPLETED, FINISHED,
              "If shutdown() method was called before.")
          .build();

      this.isDescriptionUpdated = false;
      this.isFirstIteration = true;
      this.newReceiverIdList = new ArrayList<>();
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
      if (isFirstIteration) {
        if (initialized) {
          sendSenderCanSendMessage(senderId);
        } else {
          initializedSenderIdList.add(senderId);
        }
      } else {
        numNewInitializedSenders++;
        if (isAllSendersAndReceiversUpdated() && isAllNewSendersAndReceiversInitialized()) {
          onAllSendersAndReceiversUpdated();
        }
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
      if (isFirstIteration) {
        stateMachine.checkState(INIT);
        receiverStateMachineMap.get(receiverId).checkState(PushShuffleReceiverState.RECEIVING);
        LOG.log(Level.FINE, "A receiver is initialized " + receiverId);
        numInitializedReceivers++;
        broadcastIfAllReceiversAreInitialized();
      } else {
        numNewInitializedReceivers++;
        if (isAllSendersAndReceiversUpdated() && isAllNewSendersAndReceiversInitialized()) {
          onAllSendersAndReceiversUpdated();
        }
      }
    }

    private void broadcastIfAllReceiversAreInitialized() {
      if (numInitializedReceivers == totalNumReceivers) {
        initialized = true;
        LOG.log(Level.INFO, "All receivers are initialized");
        stateMachine.checkAndSetState(INIT, CAN_SEND);
        for (final String senderId : initializedSenderIdList) {
          sendSenderCanSendMessage(senderId);
        }
      }
    }

    private synchronized void onReceiverCompleted(final String receiverId) {
      stateMachine.checkState(CAN_SEND);
      receiverStateMachineMap.get(receiverId)
          .checkAndSetState(PushShuffleReceiverState.RECEIVING, PushShuffleReceiverState.COMPLETED);

      numCompletedReceivers++;

      LOG.log(Level.FINE, "A receiver " + receiverId + " was completed to receive data.");

      if (numCompletedReceivers == totalNumReceivers) {
        numCompletedReceivers = 0;
        stateMachine.checkAndSetState(CAN_SEND, RECEIVERS_COMPLETED);
        LOG.log(Level.INFO, "All receivers were completed to receive data.");

        isFirstIteration = false;
        isDescriptionUpdated = false;
        descriptionUpdateList = new ArrayList<>();
        numUpdatedSenders = 0;
        numUpdatedReceivers = 0;
        numNewInitializedSenders = 0;
        numNewInitializedReceivers = 0;
        newSenderIdList = new ArrayList<>();
        newReceiverIdList = new ArrayList<>();
        removeSenderIdList = new ArrayList<>();
        removeReceiverIdList = new ArrayList<>();
        pushShuffleListener.onIterationCompleted(++numCompletedIterations);
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
      receiverStateMachineMap.get(receiverId)
          .checkAndSetState(PushShuffleReceiverState.COMPLETED, PushShuffleReceiverState.RECEIVING);
      numReadiedReceivers++;

      LOG.log(Level.FINE, "A receiver " + receiverId + " is ready to receive data.");

      if (numReadiedReceivers == totalNumReceivers - newReceiverIdList.size()) {
        numReadiedReceivers = 0;

        if (!isDescriptionUpdated) {
          LOG.log(Level.INFO, "Broadcast to all senders that they can send data to receivers.");
          stateMachine.checkAndSetState(RECEIVERS_COMPLETED, CAN_SEND);
          broadcastToSenders(PushShuffleCode.SENDER_CAN_SEND);
        } else {
          stateMachine.checkAndSetState(RECEIVERS_COMPLETED, READY);
          broadcastDescriptionToSendersAndReceivers(descriptionUpdateList);
        }
      }
    }

    private synchronized void onSenderUpdated() {
      numUpdatedSenders++;

      if (isAllSendersAndReceiversUpdated() && isAllNewSendersAndReceiversInitialized()) {
        onAllSendersAndReceiversUpdated();
      }
    }

    private synchronized void onReceiverUpdated() {
      numUpdatedReceivers++;

      if (isAllSendersAndReceiversUpdated() && isAllNewSendersAndReceiversInitialized()) {
        onAllSendersAndReceiversUpdated();
      }
    }

    private boolean isAllSendersAndReceiversUpdated() {
      return numUpdatedSenders == totalNumSenders - newSenderIdList.size()
          && numUpdatedReceivers == totalNumReceivers - newReceiverIdList.size();
    }

    private boolean isAllNewSendersAndReceiversInitialized() {
      return newSenderIdList.size() == numNewInitializedSenders
          && newReceiverIdList.size() == numNewInitializedReceivers;
    }

    private synchronized void onAllSendersAndReceiversUpdated() {
      stateMachine.checkAndSetState(READY, CAN_SEND);
      broadcastToSenders(PushShuffleCode.SENDER_CAN_SEND);
    }

    private synchronized void onReceiverFinished(final String receiverId) {
      stateMachine.checkState(RECEIVERS_COMPLETED);
      receiverStateMachineMap.get(receiverId)
          .checkAndSetState(PushShuffleReceiverState.COMPLETED, PushShuffleReceiverState.FINISHED);
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
      stateMachine.checkAndSetState(RECEIVERS_COMPLETED, FINISHED);
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

    private void addSender(final String senderId) {
      totalNumSenders++;
      newSenderIdList.add(senderId);
      descriptionUpdateList.add(ADD_SENDER_PREFIX + senderId);
      isDescriptionUpdated = true;
    }

    private void removeSender(final String senderId) {
      totalNumSenders--;
      removeSenderIdList.add(senderId);
      initializedSenderIdList.remove(senderId);
      descriptionUpdateList.add(REMOVE_SENDER_PREFIX + senderId);
      isDescriptionUpdated = true;
    }

    private void addReceiver(final String receiverId) {
      totalNumReceivers++;
      newReceiverIdList.add(receiverId);
      descriptionUpdateList.add(ADD_RECEIVER_PREFIX + receiverId);
      receiverStateMachineMap.put(receiverId, PushShuffleReceiverState.createStateMachine());
      isDescriptionUpdated = true;
    }

    private void removeReceiver(final String receiverId) {
      totalNumReceivers--;
      removeReceiverIdList.add(receiverId);
      descriptionUpdateList.add(REMOVE_RECEIVER_PREFIX + receiverId);
      receiverStateMachineMap.remove(receiverId);
      isDescriptionUpdated = true;
    }

    private void sendDescription(final String endPointId, final int code, final List<String> updateList) {
      try {
        controlMessageSender.send(endPointId, code, updateList);
      } catch (final NetworkException e) {
        throw new RuntimeException(e);
      }
    }

    private void broadcastDescriptionToSendersAndReceivers(final List<String> updateList) {
      for (final String senderId : shuffleDescription.getSenderIdList()) {
        if (!newSenderIdList.contains(senderId) && !removeSenderIdList.contains(senderId)) {
          sendDescription(senderId, PushShuffleCode.SENDER_DESCRIPTION_UPDATE, updateList);
        }
      }
      for (final String receiverId : shuffleDescription.getReceiverIdList()) {
        if (!newReceiverIdList.contains(receiverId) && !removeReceiverIdList.contains(receiverId)) {
          sendDescription(receiverId, PushShuffleCode.RECEIVER_DESCRIPTION_UPDATE, updateList);
        }
      }
    }
  }
}
