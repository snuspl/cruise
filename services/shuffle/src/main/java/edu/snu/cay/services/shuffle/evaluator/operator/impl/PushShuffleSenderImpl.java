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
package edu.snu.cay.services.shuffle.evaluator.operator.impl;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.driver.impl.PushShuffleCode;
import edu.snu.cay.services.shuffle.evaluator.ControlMessageSynchronizer;
import edu.snu.cay.services.shuffle.evaluator.operator.TupleSender;
import edu.snu.cay.services.shuffle.evaluator.ESControlMessageSender;
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleSender;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of push-based shuffle sender.
 */
public final class PushShuffleSenderImpl<K, V> implements PushShuffleSender<K, V> {

  private static final Logger LOG = Logger.getLogger(PushShuffleSenderImpl.class.getName());

  private final ShuffleDescription shuffleDescription;
  /**
   * TupleSender which sends tuples to proper receivers.
   */
  private final TupleSender<K, V> tupleSender;
  /**
   * ControlMessageSender which sends control messages to the manager and receivers.
   */
  private final ESControlMessageSender controlMessageSender;
  private final ControlMessageSynchronizer synchronizer;
  private final long senderTimeout;
  private final StateMachine stateMachine;
  private final SentMessageChecker messageChecker;
  private boolean shutdown;

  @Inject
  private PushShuffleSenderImpl(
      final ShuffleDescription shuffleDescription,
      final TupleSender<K, V> tupleSender,
      final ESControlMessageSender controlMessageSender,
      @Parameter(SenderTimeout.class) final long senderTimeout) {
    this.shuffleDescription = shuffleDescription;
    this.tupleSender = tupleSender;
    this.tupleSender.setTupleLinkListener(new TupleLinkListener());
    this.controlMessageSender = controlMessageSender;
    this.synchronizer = new ControlMessageSynchronizer();
    this.senderTimeout = senderTimeout;
    this.stateMachine = PushShuffleSenderState.createStateMachine();
    this.messageChecker = new SentMessageChecker();
    controlMessageSender.sendToManager(PushShuffleCode.SENDER_INITIALIZED);
  }

  @Override
  public void onControlMessage(final Message<ShuffleControlMessage> message) {
    final ShuffleControlMessage controlMessage = message.getData().iterator().next();
    switch (controlMessage.getCode()) {
    // Control messages from the manager.
    case PushShuffleCode.SENDER_CAN_SEND:
      synchronizer.closeLatch(controlMessage);
      break;

    case PushShuffleCode.SENDER_SHUTDOWN:
      shutdown = true;
      // forcibly close the latch for SENDER_CAN_SEND to shutdown the sender.
      synchronizer.closeLatch(new ShuffleControlMessage(PushShuffleCode.SENDER_CAN_SEND, null));
      break;

    default:
      throw new RuntimeException("Unknown code [ " + controlMessage.getCode() + " ] from " + message.getDestId());
    }
  }

  private final class TupleLinkListener implements LinkListener<Message<Tuple<K, V>>> {

    @Override
    public void onSuccess(final Message<Tuple<K, V>> message) {
      LOG.log(Level.FINE, "A ShuffleTupleMessage was successfully sent : {0}", message);
      messageChecker.messageTransferred();
    }

    @Override
    public void onException(
        final Throwable cause,
        final SocketAddress socketAddress,
        final Message<Tuple<K, V>> message) {
      LOG.log(Level.WARNING, "An exception occurred while sending a ShuffleTupleMessage. cause : {0}," +
          " socket address : {1}, message : {2}", new Object[]{cause, socketAddress, message});
      // TODO #67: failure handling.
      throw new RuntimeException("An exception occurred while sending a ShuffleTupleMessage", cause);
    }
  }

  @Override
  public List<String> sendTuple(final Tuple<K, V> tuple) {
    waitForSenderInitialized();
    stateMachine.checkState(PushShuffleSenderState.SENDING);
    final List<String> sentReceiverIdList = tupleSender.sendTuple(tuple);
    messageChecker.messageSent(sentReceiverIdList.size());
    return sentReceiverIdList;
  }

  @Override
  public List<String> sendTuple(final List<Tuple<K, V>> tupleList) {
    waitForSenderInitialized();
    stateMachine.checkState(PushShuffleSenderState.SENDING);
    final List<String> sentReceiverIdList = tupleSender.sendTuple(tupleList);
    messageChecker.messageSent(sentReceiverIdList.size());
    return sentReceiverIdList;
  }

  @Override
  public void sendTupleTo(final String receiverId, final Tuple<K, V> tuple) {
    waitForSenderInitialized();
    stateMachine.checkState(PushShuffleSenderState.SENDING);
    tupleSender.sendTupleTo(receiverId, tuple);
    messageChecker.messageSent(1);
  }

  @Override
  public void sendTupleTo(final String receiverId, final List<Tuple<K, V>> tupleList) {
    waitForSenderInitialized();
    stateMachine.checkState(PushShuffleSenderState.SENDING);
    tupleSender.sendTupleTo(receiverId, tupleList);
    messageChecker.messageSent(1);
  }

  private void waitForSenderInitialized() {
    if (stateMachine.getCurrentState().equals(PushShuffleSenderState.INIT)) {
      waitForSenderCanSendData();
      if (stateMachine.compareAndSetState(PushShuffleSenderState.INIT, PushShuffleSenderState.SENDING)) {
        throw new RuntimeException("The expected current state is different from the actual state");
      }
    }
  }

  @Override
  public boolean complete() {
    LOG.log(Level.INFO, "Complete to send data");
    if (stateMachine.compareAndSetState(PushShuffleSenderState.SENDING, PushShuffleSenderState.COMPLETED)) {
      throw new RuntimeException("The expected current state is different from the actual state");
    }
    messageChecker.waitForAllMessagesAreTransferred();
    LOG.log(Level.INFO, "Broadcast to all receivers that the sender was completed to send data");
    for (final String receiverId : shuffleDescription.getReceiverIdList()) {
      controlMessageSender.sendTo(receiverId, PushShuffleCode.SENDER_COMPLETED);
    }

    waitForSenderCanSendData();
    if (shutdown) {
      LOG.log(Level.INFO, "The sender was finished.");
      if (stateMachine.compareAndSetState(PushShuffleSenderState.COMPLETED, PushShuffleSenderState.FINISHED)) {
        throw new RuntimeException("The expected current state is different from the actual state");
      }
      controlMessageSender.sendToManager(PushShuffleCode.SENDER_FINISHED);
      return true;
    } else {
      LOG.log(Level.INFO, "The sender can send data");
      if (stateMachine.compareAndSetState(PushShuffleSenderState.COMPLETED, PushShuffleSenderState.SENDING)) {
        throw new RuntimeException("The expected current state is different from the actual state");
      }
      return false;
    }
  }

  private void waitForSenderCanSendData() {
    final Optional<ShuffleControlMessage> message = synchronizer.waitOnLatch(
        PushShuffleCode.SENDER_CAN_SEND, senderTimeout);

    if (message.isPresent()) {
      synchronizer.reopenLatch(PushShuffleCode.SENDER_CAN_SEND);
    } else {
      // TODO #33: failure handling
      throw new RuntimeException("the specified time elapsed but the manager did not send an expected message.");
    }
  }

  /**
   * Checker that checks all sent messages in one iteration are really transferred.
   */
  private final class SentMessageChecker {
    private int transferredMessageCount;
    private int sentMessageCount;
    private boolean waitingAllMessageTransferred;

    private synchronized void messageSent(final int num) {
      sentMessageCount += num;
    }

    private synchronized void messageTransferred() {
      transferredMessageCount++;

      if (waitingAllMessageTransferred && transferredMessageCount == sentMessageCount) {
        this.notify();
      }
    }

    private void waitForAllMessagesAreTransferred() {
      synchronized (this) {
        if (transferredMessageCount != sentMessageCount) {
          waitingAllMessageTransferred = true;
          try {
            this.wait();
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }

      waitingAllMessageTransferred = false;
      transferredMessageCount = 0;
      sentMessageCount = 0;
    }
  }

  // default_value = 10 min
  @NamedParameter(doc = "the maximum time to wait message in milliseconds.", default_value = "600000")
  public static final class SenderTimeout implements Name<Long> {
  }
}
