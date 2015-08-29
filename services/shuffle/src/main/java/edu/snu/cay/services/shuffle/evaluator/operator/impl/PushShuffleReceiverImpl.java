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

  /**
   * ControlMessageSender which sends control messages to the manager.
   */
  private final ESControlMessageSender controlMessageSender;

  private final ControlMessageSynchronizer synchronizer;
  private final long receiverTimeout;
  private final AtomicInteger finishedSenderCount;
  private final AtomicInteger completedSenderCount;
  private final int senderNum;
  private final Map<String, StateMachine> senderStateMachineMap;
  private final StateMachine stateMachine;

  private PushDataListener<K, V> dataListener;

  @Inject
  private PushShuffleReceiverImpl(
      final ShuffleDescription shuffleDescription,
      final DataReceiver<K, V> dataReceiver,
      final ESControlMessageSender controlMessageSender,
      final ControlMessageSynchronizer synchronizer,
      @Parameter(ReceiverTimeout.class) final long receiverTimeout) {
    dataReceiver.registerTupleMessageHandler(new TupleMessageHandler());
    this.controlMessageSender = controlMessageSender;
    this.synchronizer = synchronizer;
    this.receiverTimeout = receiverTimeout;
    this.finishedSenderCount = new AtomicInteger();
    this.completedSenderCount = new AtomicInteger();

    final List<String> senderIdList = shuffleDescription.getSenderIdList();
    this.senderNum = senderIdList.size();
    this.senderStateMachineMap = new HashMap<>(senderNum);
    for (final String senderId : senderIdList) {
      final StateMachine senderStateMachine = PushShuffleSenderImpl.createStateMachine();
      senderStateMachine.setState("SENDING");
      senderStateMachineMap.put(senderId, senderStateMachine);
    }

    this.stateMachine = createStateMachine();
    controlMessageSender.sendToManager(PushShuffleCode.RECEIVER_INITIALIZED);
  }

  /**
   * @return a state machine for PushShuffleReceiverImpl
   */
  public static StateMachine createStateMachine() {
    return StateMachine.newBuilder()
        .addState("RECEIVING", "Receiving data from senders."
            + " It sends a RECEIVER_INITIALIZED message to the manager when it is initialized.")
        .addState("COMPLETED", "Completed to receive data from all senders in one iteration")
        .addState("FINISHED", "Finished receiving data")
        .setInitialState("RECEIVING")
        .addTransition("RECEIVING", "COMPLETED",
            "When SENDER_COMPLETED messages arrived from all senders."
                + " It sends RECEIVER_COMPLETE messages to the manager.")
        .addTransition("COMPLETED", "RECEIVING",
            "When a ALL_RECEIVERS_COMPLETED message arrived from the manager."
                + " It sends a RECEIVER_READY message to the manager.")
        .addTransition("RECEIVING", "FINISHED",
            "When SENDER_FINISHED messages arrived from all senders."
                + "It sends a RECEIVER_FINISHED to the manager.")
        .build();
  }

  @Override
  public void registerDataListener(final PushDataListener<K, V> listener) {
    this.dataListener = listener;
  }

  @Override
  public void onControlMessage(final Message<ShuffleControlMessage> message) {
    final ShuffleControlMessage controlMessage = message.getData().iterator().next();
    switch (controlMessage.getCode()) {
    case PushShuffleCode.SENDER_COMPLETED:
      onSenderCompleted(message.getSrcId().toString());
      break;

    case PushShuffleCode.SENDER_FINISHED:
      onSenderFinished(message.getSrcId().toString());
      break;

    default:
      throw new RuntimeException("Unknown code [ " + controlMessage.getCode() + " ] from " + message.getDestId());
    }
  }

  private void onSenderCompleted(final String senderId) {
    stateMachine.checkState("RECEIVING");
    LOG.log(Level.FINE, senderId + " was completed to send data");
    senderStateMachineMap.get(senderId).checkAndSetState("SENDING", "COMPLETED");
    if (completedSenderCount.incrementAndGet() == senderNum) {
      onAllSendersCompleted();
    }
  }

  private void onAllSendersCompleted() {
    stateMachine.checkAndSetState("RECEIVING", "COMPLETED");
    completedSenderCount.set(0);
    LOG.log(Level.INFO, "All senders were completed to send data.");
    for (final StateMachine senderStateMachine : senderStateMachineMap.values()) {
      senderStateMachine.checkAndSetState("COMPLETED", "SENDING");
    }

    dataListener.onComplete(false);
    controlMessageSender.sendToManager(PushShuffleCode.RECEIVER_COMPLETED);

    final Optional<ShuffleControlMessage> message = synchronizer.waitOnLatch(
        PushShuffleCode.ALL_RECEIVERS_COMPLETED, receiverTimeout);
    if (message.isPresent()) {
      synchronizer.reopenLatch(PushShuffleCode.ALL_RECEIVERS_COMPLETED);
    } else {
      // TODO #33: failure handling
      throw new RuntimeException("the specified time elapsed but the manager did not send an expected message.");
    }

    stateMachine.checkAndSetState("COMPLETED", "RECEIVING");
    controlMessageSender.sendToManager(PushShuffleCode.RECEIVER_READY);
  }

  private void onSenderFinished(final String senderId) {
    stateMachine.checkState("RECEIVING");
    LOG.log(Level.FINE, senderId + " was finished sending data");
    senderStateMachineMap.get(senderId).checkAndSetState("SENDING", "FINISHED");
    if (finishedSenderCount.incrementAndGet() == senderNum) {
      onAllSendersFinished();
    }
  }

  private void onAllSendersFinished() {
    stateMachine.checkAndSetState("RECEIVING", "FINISHED");
    LOG.log(Level.INFO, "All senders were finished sending data");
    controlMessageSender.sendToManager(PushShuffleCode.RECEIVER_FINISHED);
    dataListener.onComplete(true);
  }

  private final class TupleMessageHandler implements EventHandler<Message<ShuffleTupleMessage<K, V>>> {

    @Override
    public void onNext(final Message<ShuffleTupleMessage<K, V>> message) {
      stateMachine.checkState("RECEIVING");
      dataListener.onTupleMessage(message);
    }
  }

  // default_value = 10 min
  @NamedParameter(doc = "the maximum time to wait message in milliseconds.", default_value = "600000")
  public static final class ReceiverTimeout implements Name<Long> {
  }
}
