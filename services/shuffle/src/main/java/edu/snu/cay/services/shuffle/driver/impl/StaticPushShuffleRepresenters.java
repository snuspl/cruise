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
import edu.snu.cay.services.shuffle.evaluator.operator.impl.PushShuffleReceiverImpl;
import edu.snu.cay.services.shuffle.evaluator.operator.impl.PushShuffleSenderImpl;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-side representation of all static push shuffle components on an evaluator.
 */
@DriverSide
final class StaticPushShuffleRepresenters {

  private static final Logger LOG = Logger.getLogger(StaticPushShuffleRepresenters.class.getName());

  private final DSControlMessageSender controlMessageSender;
  private final int senderNum;
  private final int receiverNum;
  private Map<String, SenderRepresenter> senderRepresenterMap;
  private Map<String, ReceiverRepresenter> receiverRepresenterMap;
  private int initializedSenderNum;
  private int completedSenderNum;
  private int initializedReceiverNum;
  private int receivedReceiverNum;

  private State currentState;

  @Inject
  private StaticPushShuffleRepresenters(
      final ShuffleDescription shuffleDescription,
      final DSControlMessageSender controlMessageSender) {
    this.controlMessageSender = controlMessageSender;
    this.senderNum = shuffleDescription.getSenderIdList().size();
    this.receiverNum = shuffleDescription.getReceiverIdList().size();
    this.senderRepresenterMap = new HashMap<>(senderNum);
    this.receiverRepresenterMap = new HashMap<>(receiverNum);

    for (final String senderId : shuffleDescription.getSenderIdList()) {
      senderRepresenterMap.put(senderId, new SenderRepresenter());
    }

    for (final String receiverId : shuffleDescription.getReceiverIdList()) {
      receiverRepresenterMap.put(receiverId, new ReceiverRepresenter());
    }

    this.currentState = State.STARTED;
  }


  private synchronized void onSenderInitialized(final String senderId) {
    senderRepresenterMap.get(senderId)
        .checkAndSetState(PushShuffleSenderImpl.State.STARTED, PushShuffleSenderImpl.State.SENDING);
    LOG.log(Level.FINE, "A sender is initialized " + senderId);
    initializedSenderNum++;
    broadcastIfShuffleIsInitialized();
  }

  private synchronized void onReceiverInitialized(final String receiverId) {
    receiverRepresenterMap.get(receiverId)
        .checkAndSetState(PushShuffleReceiverImpl.State.STARTED, PushShuffleReceiverImpl.State.RECEIVING);
    LOG.log(Level.FINE, "A receiver is initialized " + receiverId);
    initializedReceiverNum++;
    broadcastIfShuffleIsInitialized();
  }

  private void broadcastIfShuffleIsInitialized() {
    if (initializedSenderNum == senderNum && initializedReceiverNum == receiverNum) {
      LOG.log(Level.INFO, "Broadcast to all end points that the shuffle is initialized");
      checkAndSetState(State.STARTED, State.SENDING);
      broadcastToSender(PushShuffleCode.SHUFFLE_INITIALIZED);
      broadcastToReceiver(PushShuffleCode.SHUFFLE_INITIALIZED);
    }
  }

  private synchronized void onSenderCompleted(final String senderId) {
    senderRepresenterMap.get(senderId)
        .checkAndSetState(PushShuffleSenderImpl.State.SENDING, PushShuffleSenderImpl.State.WAITING);
    completedSenderNum++;

    LOG.log(Level.FINE, "A sender completed to send tuples " + senderId);

    if (completedSenderNum == senderNum) {
      completedSenderNum = 0;
      LOG.log(Level.INFO, "Broadcast to all receivers that all senders are completed to send tuples");
      broadcastToReceiver(PushShuffleCode.ALL_SENDERS_COMPLETED);
      checkAndSetState(State.SENDING, State.RECEIVING);
    }
  }

  private synchronized void onReceiverReceived(final String receiverId) {
    receiverRepresenterMap.get(receiverId)
        .checkAndSetState(PushShuffleReceiverImpl.State.RECEIVING, PushShuffleReceiverImpl.State.RECEIVING);
    receivedReceiverNum++;

    LOG.log(Level.FINE, "A receiver received all tuples from senders " + receiverId);

    if (receivedReceiverNum == receiverNum) {
      receivedReceiverNum = 0;
      LOG.log(Level.INFO, "Broadcast to all senders that all receivers are received from senders");
      broadcastToSender(PushShuffleCode.ALL_RECEIVERS_RECEIVED);
      checkAndSetAllSenderRepresentersState(PushShuffleSenderImpl.State.WAITING, PushShuffleSenderImpl.State.SENDING);
      checkAndSetState(State.RECEIVING, State.SENDING);
    }
  }

  private synchronized void checkAndSetAllSenderRepresentersState(
      final PushShuffleSenderImpl.State from, final PushShuffleSenderImpl.State to) {
    for (final SenderRepresenter senderRepresenter : senderRepresenterMap.values()) {
      senderRepresenter.checkAndSetState(from, to);
    }
  }

  private void broadcastToSender(final int code) {
    for (final String sender : senderRepresenterMap.keySet()) {
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
    for (final String receiver : receiverRepresenterMap.keySet()) {
      try {
        controlMessageSender.send(receiver, code);
      } catch (final NetworkException e) {
        // cannot open connection to sender
        // TODO (#67) : failure handling.
        throw new RuntimeException(e);
      }
    }
  }

  void onControlMessage(final String endPointId, final ShuffleControlMessage controlMessage) {
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

  private synchronized void checkState(final State expectedState) {
    if (currentState != expectedState) {
      throw new IllegalStateException("Expected state is " + expectedState
          + " but the current state is " + currentState);
    }
  }

  static boolean isLegalTransition(final State from, final State to) {
    switch (from) {
    case STARTED:
      switch (to) {
      case SENDING:
        return true;
      default:
        return false;
      }
    case SENDING:
      switch (to) {
      case RECEIVING:
        return true;
      default:
        return false;
      }
    case RECEIVING:
      switch (to) {
      case SENDING:
        return true;
      default:
        return false;
      }
    default:
      throw new RuntimeException("Unknown state : " + from);
    }
  }

  private synchronized void setState(final State state) {
    if (isLegalTransition(currentState, state)) {
      currentState = state;
    } else {
      throw new IllegalStateException("Illegal state transition from " + currentState + " to " + state);
    }
  }

  private synchronized void checkAndSetState(final State expectedState, final State state) {
    checkState(expectedState);
    setState(state);
  }

  enum State {
    STARTED,
    SENDING,
    RECEIVING,
  }

  private final class SenderRepresenter {
    private PushShuffleSenderImpl.State currentState;

    private SenderRepresenter() {
      this.currentState = PushShuffleSenderImpl.State.STARTED;
    }

    private synchronized void checkAndSetState(
        final PushShuffleSenderImpl.State expectedState,
        final PushShuffleSenderImpl.State state) {
      if (currentState != expectedState) {
        throw new IllegalStateException("Expected current state is " + expectedState
            + " but the current state is " + currentState);
      }

      if (PushShuffleSenderImpl.isLegalTransition(currentState, state)) {
        currentState = state;
      } else {
        throw new IllegalStateException("Illegal state transition from " + currentState + " to " + state);
      }
    }
  }

  private final class ReceiverRepresenter {
    private PushShuffleReceiverImpl.State currentState;

    private ReceiverRepresenter() {
      this.currentState = PushShuffleReceiverImpl.State.STARTED;
    }

    private synchronized void checkAndSetState(
        final PushShuffleReceiverImpl.State expectedCurrentState,
        final PushShuffleReceiverImpl.State state) {
      if (currentState != expectedCurrentState) {
        throw new IllegalStateException("Expected current state is " + expectedCurrentState
            + " but the current state is " + currentState);
      }

      if (PushShuffleReceiverImpl.isLegalTransition(currentState, state)) {
        currentState = state;
      } else {
        throw new IllegalStateException("Illegal state transition from " + currentState + " to " + state);
      }
    }
  }
}
