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

import edu.snu.cay.services.shuffle.driver.impl.PushShuffleCode;
import edu.snu.cay.services.shuffle.evaluator.ControlMessageSynchronizer;
import edu.snu.cay.services.shuffle.evaluator.DataReceiver;
import edu.snu.cay.services.shuffle.evaluator.ESControlMessageSender;
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleReceiver;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public final class PushShuffleReceiverImpl<K, V> implements PushShuffleReceiver<K, V> {

  private static final Logger LOG = Logger.getLogger(PushShuffleReceiverImpl.class.getName());

  private final ESControlMessageSender controlMessageSender;
  private final ControlMessageSynchronizer synchronizer;
  private final AtomicBoolean initialized;
  private final List<Tuple<K, V>> receivedTupleList;

  private State currentState;

  @Inject
  private PushShuffleReceiverImpl(
      final DataReceiver<K, V> dataReceiver,
      final ESControlMessageSender controlMessageSender,
      final ControlMessageSynchronizer synchronizer) {
    dataReceiver.registerTupleMessageHandler(new TupleMessageHandler());
    this.controlMessageSender = controlMessageSender;
    this.synchronizer = synchronizer;
    this.initialized = new AtomicBoolean();
    this.receivedTupleList = new ArrayList<>();
    this.currentState = State.STARTED;

    controlMessageSender.sendToManager(PushShuffleCode.RECEIVER_INITIALIZED);
  }

  @Override
  public Iterable<Tuple<K, V>> receive() {
    waitForInitializing();
    checkState(State.RECEIVING);
    LOG.log(Level.INFO, "Wait for all senders are completed");
    synchronizer.waitOnLatch(PushShuffleCode.ALL_SENDERS_COMPLETED);
    synchronizer.reopenLatch(PushShuffleCode.ALL_SENDERS_COMPLETED);
    final List<Tuple<K, V>> copiedList;
    synchronized (receivedTupleList) {
      copiedList = new ArrayList<>(receivedTupleList.size());
      copiedList.addAll(receivedTupleList);
      receivedTupleList.clear();
    }

    controlMessageSender.sendToManager(PushShuffleCode.RECEIVER_RECEIVED);
    return copiedList;
  }

  private final class TupleMessageHandler implements EventHandler<Message<ShuffleTupleMessage<K, V>>> {

    @Override
    public void onNext(final Message<ShuffleTupleMessage<K, V>> message) {
      synchronized (receivedTupleList) {
        for (final ShuffleTupleMessage<K, V> shuffleTupleMessage : message.getData()) {
          for (int i = 0; i < shuffleTupleMessage.size(); i++) {
            receivedTupleList.add(shuffleTupleMessage.get(i));
          }
        }
      }
    }
  }

  private void waitForInitializing() {
    if (initialized.compareAndSet(false, true)) {
      synchronizer.waitOnLatch(PushShuffleCode.SHUFFLE_INITIALIZED);
      checkAndSetState(State.STARTED, State.RECEIVING);
    }
  }

  private synchronized void checkState(final State expectedState) {
    if (currentState != expectedState) {
      throw new IllegalStateException("Expected state is "
          + expectedState + " but the current state is " + currentState);
    }
  }

  public static boolean isLegalTransition(final State from, final State to) {
    switch (from) {
    case STARTED:
      switch (to) {
      case RECEIVING:
        return true;
      default:
        return false;
      }
    case RECEIVING:
      switch (to) {
      case RECEIVING:
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

  public enum State {
    STARTED,
    RECEIVING
  }
}
