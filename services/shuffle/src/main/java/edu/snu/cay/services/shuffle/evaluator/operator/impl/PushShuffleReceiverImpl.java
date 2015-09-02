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
import edu.snu.cay.services.shuffle.evaluator.DataReceiver;
import edu.snu.cay.services.shuffle.evaluator.ESControlMessageSender;
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.PushDataListener;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import edu.snu.cay.services.shuffle.utils.StateMachine;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation for push-based shuffle receiver.
 */
public final class PushShuffleReceiverImpl<K, V> implements PushShuffleReceiver<K, V> {

  private static final Logger LOG = Logger.getLogger(PushShuffleReceiverImpl.class.getName());

  private final ShuffleDescription shuffleDescription;
  /**
   * ControlMessageSender which sends control messages to the manager.
   */
  private final ESControlMessageSender controlMessageSender;

  private final ControlMessageSynchronizer synchronizer;
  private final long receiverTimeout;
  private final AtomicInteger completedSenderCount;
  private final int totalNumSenders;
  private final Map<String, StateMachine> senderStateMachineMap;
  private final StateMachine stateMachine;

  private boolean shutdown;
  private PushDataListener<K, V> dataListener;

  @Inject
  private PushShuffleReceiverImpl(
      final ShuffleDescription shuffleDescription,
      final DataReceiver<K, V> dataReceiver,
      final ESControlMessageSender controlMessageSender,
      @Parameter(ReceiverTimeout.class) final long receiverTimeout) {
    dataReceiver.registerTupleMessageHandler(new TupleMessageHandler());
    this.shuffleDescription = shuffleDescription;
    this.controlMessageSender = controlMessageSender;
    this.synchronizer = new ControlMessageSynchronizer();
    this.receiverTimeout = receiverTimeout;
    this.completedSenderCount = new AtomicInteger();

    final List<String> senderIdList = shuffleDescription.getSenderIdList();
    this.totalNumSenders = senderIdList.size();
    this.senderStateMachineMap = new HashMap<>(totalNumSenders);
    for (final String senderId : senderIdList) {
      final StateMachine senderStateMachine = PushShuffleSenderState.createStateMachine();
      senderStateMachine.setState(PushShuffleSenderState.SENDING);
      senderStateMachineMap.put(senderId, senderStateMachine);
    }

    this.stateMachine = PushShuffleReceiverState.createStateMachine();
    controlMessageSender.sendToManager(PushShuffleCode.RECEIVER_INITIALIZED);
  }

  @Override
  public void registerDataListener(final PushDataListener<K, V> listener) {
    this.dataListener = listener;
  }

  @Override
  public void onControlMessage(final Message<ShuffleControlMessage> message) {
    final ShuffleControlMessage controlMessage = message.getData().iterator().next();
    switch (controlMessage.getCode()) {
    // Control messages from the manager.
    case PushShuffleCode.RECEIVER_CAN_RECEIVE:
      synchronizer.closeLatch(controlMessage);
      break;

    case PushShuffleCode.RECEIVER_SHUTDOWN:
      shutdown = true;
      // forcibly close the latch for RECEIVER_CAN_RECEIVE to shutdown the receiver.
      synchronizer.closeLatch(new ShuffleControlMessage(
          PushShuffleCode.RECEIVER_CAN_RECEIVE, shuffleDescription.getShuffleName(), null));
      break;

    // Control messages from senders.
    case PushShuffleCode.SENDER_COMPLETED:
      onSenderCompleted(message.getSrcId().toString());
      break;

    default:
      throw new RuntimeException("Unknown code [ " + controlMessage.getCode() + " ] from " + message.getDestId());
    }
  }

  private void onSenderCompleted(final String senderId) {
    stateMachine.checkState(PushShuffleReceiverState.RECEIVING);
    LOG.log(Level.FINE, senderId + " was completed to send data");
    senderStateMachineMap.get(senderId)
        .checkAndSetState(PushShuffleSenderState.SENDING, PushShuffleSenderState.COMPLETED);
    if (completedSenderCount.incrementAndGet() == totalNumSenders) {
      onAllSendersCompleted();
    }
  }

  private void onAllSendersCompleted() {
    stateMachine.checkAndSetState(PushShuffleReceiverState.RECEIVING, PushShuffleReceiverState.COMPLETED);
    completedSenderCount.set(0);
    LOG.log(Level.INFO, "All senders were completed to send data.");
    for (final StateMachine senderStateMachine : senderStateMachineMap.values()) {
      senderStateMachine.checkAndSetState(PushShuffleSenderState.COMPLETED, PushShuffleSenderState.SENDING);
    }

    controlMessageSender.sendToManager(PushShuffleCode.RECEIVER_COMPLETED);

    final Optional<ShuffleControlMessage> message = synchronizer.waitOnLatch(
        PushShuffleCode.RECEIVER_CAN_RECEIVE, receiverTimeout);
    if (message.isPresent()) {
      synchronizer.reopenLatch(PushShuffleCode.RECEIVER_CAN_RECEIVE);
    } else {
      // TODO #33: failure handling
      throw new RuntimeException("the specified time elapsed but the manager did not send an expected message.");
    }

    if (shutdown) {
      LOG.log(Level.INFO, "The receiver was finished.");
      stateMachine.checkAndSetState(PushShuffleReceiverState.COMPLETED, PushShuffleReceiverState.FINISHED);
      dataListener.onComplete();
      controlMessageSender.sendToManager(PushShuffleCode.RECEIVER_FINISHED);
      dataListener.onShutdown();
    } else {
      LOG.log(Level.INFO, "The receiver can receive data");
      stateMachine.checkAndSetState(PushShuffleReceiverState.COMPLETED, PushShuffleReceiverState.RECEIVING);
      dataListener.onComplete();
      controlMessageSender.sendToManager(PushShuffleCode.RECEIVER_READIED);
    }
  }

  private final class TupleMessageHandler implements EventHandler<Message<ShuffleTupleMessage<K, V>>> {

    @Override
    public void onNext(final Message<ShuffleTupleMessage<K, V>> message) {
      stateMachine.checkState(PushShuffleReceiverState.RECEIVING);
      dataListener.onTupleMessage(message);
    }
  }

  // default_value = 10 min
  @NamedParameter(doc = "the maximum time to wait message in milliseconds.", default_value = "600000")
  public static final class ReceiverTimeout implements Name<Long> {
  }
}
