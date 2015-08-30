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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
      case PushShuffleCode.RECEIVER_INITIALIZED:
        stateManager.onReceiverInitialized(endPointId);
        break;
      case PushShuffleCode.RECEIVER_COMPLETED:
        stateManager.onReceiverCompleted(endPointId);
        break;
      case PushShuffleCode.RECEIVER_READY:
        stateManager.onReceiverReady(endPointId);
        break;
      case PushShuffleCode.RECEIVER_SHUTDOWN:
        stateManager.onReceiverFinished(endPointId);
        break;
      default:
        throw new IllegalStateException("Unknown code " + controlMessage.getCode() + " from " + endPointId);
      }
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

    private boolean initialized;
    private boolean shutdown;
    private boolean shutdownAllSendersAndReceivers;
    private final int receiverNum;
    private Map<String, StateMachine> receiverStateMachineMap;
    private final List<String> initializedSenderIdList;
    private int initializedReceiverNum;
    private int readiedReceiverNum;
    private int completedReceiverNum;
    private int finishedReceiverNum;

    private StateMachine stateMachine;

    private StateManager() {
      this.initializedSenderIdList = new ArrayList<>();
      this.receiverNum = shuffleDescription.getReceiverIdList().size();
      this.receiverStateMachineMap = new HashMap<>(receiverNum);

      for (final String receiverId : shuffleDescription.getReceiverIdList()) {
        receiverStateMachineMap.put(receiverId, PushShuffleReceiverImpl.createStateMachine());
      }

      this.stateMachine = StateMachine.newBuilder()
          .addState("INIT", "Wait for all receivers are initialized")
          .addState("CAN_SEND", "Prepared senders can send data to receivers")
          .addState("RECEIVERS_COMPLETED", "All receivers were completed to receive data from senders in an iteration")
          .addState("FINISHED", "All senders and receivers were finished")
          .setInitialState("INIT")
          .addTransition("INIT", "CAN_SEND",
              "When RECEIVER_INITIALIZED messages arrived from all receivers."
                  + " It sends SENDER_CAN_SEND messages to all initialized senders.")
          .addTransition("CAN_SEND", "RECEIVERS_COMPLETED",
              "When RECEIVER_COMPLETED messages arrived from all receivers." +
                  " It broadcasts ALL_RECEIVERS_COMPLETED messages to all receivers.")
          .addTransition("RECEIVERS_COMPLETED", "CAN_SEND",
              "When RECEIVER_READY messages arrived from all receivers"
              + " It broadcasts SENDER_CAN_SEND messages to all senders.")
          .addTransition("RECEIVERS_COMPLETED", "FINISHED",
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

    private synchronized void onReceiverInitialized(final String receiverId) {
      stateMachine.checkState("INIT");
      receiverStateMachineMap.get(receiverId).checkState("RECEIVING");
      LOG.log(Level.FINE, "A receiver is initialized " + receiverId);
      initializedReceiverNum++;
      broadcastIfAllReceiversAreInitialized();
    }

    private void broadcastIfAllReceiversAreInitialized() {
      if (initializedReceiverNum == receiverNum) {
        initialized = true;
        LOG.log(Level.INFO, "All receivers are initialized");
        stateMachine.checkAndSetState("INIT", "CAN_SEND");
        for (final String senderId : initializedSenderIdList) {
          sendSenderCanSendMessage(senderId);
        }
      }
    }

    private synchronized void onReceiverCompleted(final String receiverId) {
      stateMachine.checkState("CAN_SEND");
      receiverStateMachineMap.get(receiverId).checkAndSetState("RECEIVING", "COMPLETED");
      if (completedReceiverNum == 0 && shutdown) {
        shutdownAllSendersAndReceivers = true;
      }

      completedReceiverNum++;

      LOG.log(Level.FINE, "A receiver " + receiverId + " was completed to receive data.");

      try {
        if (shutdownAllSendersAndReceivers) {
          controlMessageSender.send(receiverId, PushShuffleCode.RECEIVER_SHUTDOWN);
        } else {
          controlMessageSender.send(receiverId, PushShuffleCode.RECEIVER_CAN_RECEIVE);
        }
      } catch (final NetworkException e) {
        // cannot open connection to receiver
        // TODO #67: failure handling.
        throw new RuntimeException(e);
      }

      if (completedReceiverNum == receiverNum) {
        completedReceiverNum = 0;
        stateMachine.checkAndSetState("CAN_SEND", "RECEIVERS_COMPLETED");
        LOG.log(Level.INFO, "All receivers were completed to receive data.");

        if (shutdownAllSendersAndReceivers) {
          shutdownAllSenders();
        }
      }
    }

    private void shutdownAllSenders() {
      stateMachine.checkAndSetState("RECEIVERS_COMPLETED", "FINISHED");
      broadcastToSenders(PushShuffleCode.SENDER_SHUTDOWN);
    }

    private synchronized void onReceiverReady(final String receiverId) {
      receiverStateMachineMap.get(receiverId).checkAndSetState("COMPLETED", "RECEIVING");
      readiedReceiverNum++;

      LOG.log(Level.FINE, "A receiver " + receiverId + " is ready to receive data.");

      if (readiedReceiverNum == receiverNum) {
        readiedReceiverNum = 0;
        LOG.log(Level.INFO, "Broadcast to all senders that they can send data to receivers.");
        stateMachine.checkAndSetState("RECEIVERS_COMPLETED", "CAN_SEND");
        broadcastToSenders(PushShuffleCode.SENDER_CAN_SEND);
      }
    }

    private synchronized void onReceiverFinished(final String receiverId) {
      stateMachine.checkState("CAN_SEND");
      receiverStateMachineMap.get(receiverId).checkAndSetState("RECEIVING", "FINISHED");
      finishedReceiverNum++;


      if (finishedReceiverNum == receiverNum) {
        LOG.log(Level.INFO, "The StaticPushShuffleManager is finished");
        stateMachine.checkAndSetState("CAN_SEND", "FINISHED");
      }
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
  }
}
