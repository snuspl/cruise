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
import edu.snu.cay.services.shuffle.evaluator.operator.impl.PushShuffleSenderImpl;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.services.shuffle.utils.ShuffleDescriptionSerializer;
import edu.snu.cay.services.shuffle.utils.StateMachine;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ShuffleManager implementation for static push-based shuffling.
 *
 * The initial shuffle description can never be changed. Users cannot add or remove more tasks
 * to the shuffle and cannot change the key, value codecs and shuffling strategy after the manager is created.
 */
@DriverSide
public final class StaticPushShuffleManager implements ShuffleManager {

  private static final Logger LOG = Logger.getLogger(StaticPushShuffleManager.class.getName());

  private final ShuffleDescription shuffleDescription;
  private final ShuffleDescriptionSerializer descriptionSerializer;
  private final DSControlMessageSender controlMessageSender;

  private final ControlMessageHandler controlMessageHandler;
  private final ControlLinkListener controlLinkListener;
  private final StateManager stateManager;

  @Inject
  private StaticPushShuffleManager(
      final ShuffleDescription shuffleDescription,
      final ShuffleDescriptionSerializer descriptionSerializer,
      final DSControlMessageSender controlMessageSender) {
    this.shuffleDescription = shuffleDescription;
    this.descriptionSerializer = descriptionSerializer;
    this.controlMessageSender = controlMessageSender;

    this.stateManager = new StateManager();
    this.controlMessageHandler = new ControlMessageHandler();
    this.controlLinkListener = new ControlLinkListener();
  }

  /**
   * @param endPointId end point id
   * @return Serialized shuffle description for the endPointId
   */
  @Override
  public Optional<Configuration> getShuffleConfiguration(final String endPointId) {
    return descriptionSerializer.serialize(
        StaticPushShuffle.class,
        PushShuffleSenderImpl.class,
        PushShuffleReceiverImpl.class,
        shuffleDescription, endPointId);
  }

  /**
   * @return the initial shuffle description
   */
  @Override
  public ShuffleDescription getShuffleDescription() {
    return shuffleDescription;
  }

  @Override
  public EventHandler<Message<ShuffleControlMessage>> getControlMessageHandler() {
    return controlMessageHandler;
  }

  @Override
  public LinkListener<Message<ShuffleControlMessage>> getControlLinkListener() {
    return controlLinkListener;
  }

  private final class ControlMessageHandler implements EventHandler<Message<ShuffleControlMessage>> {

    @Override
    public void onNext(final Message<ShuffleControlMessage> message) {
      final ShuffleControlMessage controlMessage = message.getData().iterator().next();
      stateManager.onControlMessage(message.getSrcId().toString(), controlMessage);
    }
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
      // TODO (#67) : failure handling.
      throw new RuntimeException("An exception occurred while sending a ShuffleControlMessage", cause);
    }
  }

  /**
   * State manager for StaticPushShuffleManager. This receives control messages
   * from senders and receivers and makes a proper state transition as communicating
   * with senders and receivers.
   */
  private final class StateManager {

    private final int senderNum;
    private final int receiverNum;
    private Map<String, StateMachine> senderStateMachineMap;
    private Map<String, StateMachine> receiverStateMachineMap;
    private int initializedSenderNum;
    private int completedSenderNum;
    private int initializedReceiverNum;
    private int receivedReceiverNum;

    private StateMachine stateMachine;

    private StateManager() {
      this.senderNum = shuffleDescription.getSenderIdList().size();
      this.receiverNum = shuffleDescription.getReceiverIdList().size();
      this.senderStateMachineMap = new HashMap<>(senderNum);
      this.receiverStateMachineMap = new HashMap<>(receiverNum);

      for (final String senderId : shuffleDescription.getSenderIdList()) {
        senderStateMachineMap.put(senderId, PushShuffleSenderImpl.createStateMachine());
      }

      for (final String receiverId : shuffleDescription.getReceiverIdList()) {
        receiverStateMachineMap.put(receiverId, PushShuffleReceiverImpl.createStateMachine());
      }

      this.stateMachine = StateMachine.newBuilder()
          .addState("CREATED", "Wait for initialized messages from all senders and receivers")
          .addState("SENDING", "Senders are sending tuples to receivers")
          .addState("RECEIVING", "Wait for all receivers report that they received all tuples from senders")
          .setInitialState("CREATED")
          .addTransition("CREATED", "SENDING",
              "When SENDER_INITIALIZED messages arrived from all senders and RECEIVER_INITIALIZED"
                  + " messages arrived from all receivers."
                  + " It sends SHUFFLE_INITIALIZED messages to all senders and receivers.")
          .addTransition("SENDING", "RECEIVING",
              "When SENDER_COMPLETED messages arrived from all senders." +
                  " It sends ALL_SENDERS_COMPLETED messages to all receivers.")
          .addTransition("RECEIVING", "SENDING",
              "When RECEIVER_RECEIVED messages arrived from all receivers"
              + " It sends ALL_RECEIVERS_RECEIVED messages to all senders.")
          .build();
    }

    private synchronized void onSenderInitialized(final String senderId) {
      senderStateMachineMap.get(senderId).checkAndSetState("CREATED", "SENDING");
      LOG.log(Level.FINE, "A sender is initialized " + senderId);
      initializedSenderNum++;
      broadcastIfShuffleIsInitialized();
    }

    private synchronized void onReceiverInitialized(final String receiverId) {
      receiverStateMachineMap.get(receiverId).checkAndSetState("CREATED", "RECEIVING");
      LOG.log(Level.FINE, "A receiver is initialized " + receiverId);
      initializedReceiverNum++;
      broadcastIfShuffleIsInitialized();
    }

    private void broadcastIfShuffleIsInitialized() {
      if (initializedSenderNum == senderNum && initializedReceiverNum == receiverNum) {
        LOG.log(Level.INFO, "Broadcast to all end points that the shuffle is initialized");
        stateMachine.checkAndSetState("CREATED", "SENDING");
        broadcastToSender(PushShuffleCode.SHUFFLE_INITIALIZED);
        broadcastToReceiver(PushShuffleCode.SHUFFLE_INITIALIZED);
      }
    }

    private synchronized void onSenderCompleted(final String senderId) {
      senderStateMachineMap.get(senderId).checkAndSetState("SENDING", "WAITING");
      completedSenderNum++;

      LOG.log(Level.FINE, "A sender completed to send tuples " + senderId);

      if (completedSenderNum == senderNum) {
        completedSenderNum = 0;
        LOG.log(Level.INFO, "Broadcast to all receivers that all senders are completed to send tuples");
        broadcastToReceiver(PushShuffleCode.ALL_SENDERS_COMPLETED);
        stateMachine.checkAndSetState("SENDING", "RECEIVING");
      }
    }

    private synchronized void onReceiverReceived(final String receiverId) {
      receiverStateMachineMap.get(receiverId).checkAndSetState("RECEIVING", "RECEIVING");
      receivedReceiverNum++;

      LOG.log(Level.FINE, "A receiver received all tuples from senders " + receiverId);

      if (receivedReceiverNum == receiverNum) {
        receivedReceiverNum = 0;
        LOG.log(Level.INFO, "Broadcast to all senders that all receivers are received from senders");
        broadcastToSender(PushShuffleCode.ALL_RECEIVERS_RECEIVED);
        checkAndSetAllSenderStateMachinesState("WAITING", "SENDING");
        stateMachine.checkAndSetState("RECEIVING", "SENDING");
      }
    }

    private synchronized void checkAndSetAllSenderStateMachinesState(final String from, final String to) {
      for (final StateMachine senderStateMachine : senderStateMachineMap.values()) {
        senderStateMachine.checkAndSetState(from, to);
      }
    }

    private void broadcastToSender(final int code) {
      for (final String sender : senderStateMachineMap.keySet()) {
        try {
          controlMessageSender.send(sender, code);
        } catch (final NetworkException e) {
          // cannot open connection to sender
          // TODO (#67) : failure handling.
          throw new RuntimeException(e);
        }
      }
    }

    private void broadcastToReceiver(final int code) {
      for (final String receiver : receiverStateMachineMap.keySet()) {
        try {
          controlMessageSender.send(receiver, code);
        } catch (final NetworkException e) {
          // cannot open connection to sender
          // TODO (#67) : failure handling.
          throw new RuntimeException(e);
        }
      }
    }

    private void onControlMessage(final String endPointId, final ShuffleControlMessage controlMessage) {
      switch (controlMessage.getCode()) {
      case PushShuffleCode.SENDER_INITIALIZED:
        onSenderInitialized(endPointId);
        break;
      case PushShuffleCode.RECEIVER_INITIALIZED:
        onReceiverInitialized(endPointId);
        break;
      case PushShuffleCode.SENDER_COMPLETED:
        onSenderCompleted(endPointId);
        break;
      case PushShuffleCode.RECEIVER_RECEIVED:
        onReceiverReceived(endPointId);
        break;
      default:
        throw new IllegalStateException("Unknown code " + controlMessage.getCode() + " from " + endPointId);
      }
    }
  }
}
