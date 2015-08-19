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
import edu.snu.cay.services.shuffle.evaluator.DataSender;
import edu.snu.cay.services.shuffle.evaluator.ESControlMessageSender;
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleSender;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation for push-based shuffle sender.
 *
 * State summary.
 *
 * CREATED:
 * Wait for initialized message from the manager. This sends a SENDER_INITIALIZED message
 * to the manager when it is instantiated.
 *
 * SENDING:
 * Send tuples to receivers.
 *
 * WAITING:
 * Wait for all receivers received tuples from senders.
 *
 * Transition summary.
 *
 * CREATED -> SENDING:
 * When a SHUFFLE_INITIALIZED message arrived from the manager.
 *
 * SENDING -> WAITING:
 * When a user calls complete() method.
 * It sends a SENDER_COMPLETED message to the manager.
 *
 * WAITING -> SENDING:
 * When a ALL_RECEIVERS_RECEIVED message arrived from the manager.
 * It wakes up a caller who is blocking on waitForReceiver() or completeAndWaitForReceiver().
 */
public final class PushShuffleSenderImpl<K, V> implements PushShuffleSender<K, V> {

  private static final Logger LOG = Logger.getLogger(PushShuffleSenderImpl.class.getName());

  private final DataSender<K, V> dataSender;
  private final ESControlMessageSender controlMessageSender;
  private final ControlMessageSynchronizer synchronizer;
  private final AtomicBoolean initialized;

  private State currentState;

  @Inject
  private PushShuffleSenderImpl(
      final DataSender<K, V> dataSender,
      final ESControlMessageSender controlMessageSender,
      final ControlMessageSynchronizer synchronizer) {
    this.dataSender = dataSender;
    dataSender.registerTupleLinkListener(new TupleLinkListener());
    this.controlMessageSender = controlMessageSender;
    this.synchronizer = synchronizer;
    this.initialized = new AtomicBoolean();
    this.currentState = State.CREATED;
  }

  private final class TupleLinkListener implements LinkListener<Message<ShuffleTupleMessage<K, V>>> {

    @Override
    public void onSuccess(final Message<ShuffleTupleMessage<K, V>> message) {
      LOG.log(Level.FINE, "A ShuffleTupleMessage was successfully sent : {0}", message);
    }

    @Override
    public void onException(
        final Throwable cause,
        final SocketAddress socketAddress,
        final Message<ShuffleTupleMessage<K, V>> message) {
      LOG.log(Level.WARNING, "An exception occurred while sending a ShuffleTupleMessage. cause : {0}," +
          " socket address : {1}, message : {2}", new Object[]{cause, socketAddress, message});
      // TODO (#67) : failure handling.
      throw new RuntimeException("An exception occurred while sending a ShuffleTupleMessage", cause);
    }
  }

  @Override
  public List<String> sendTuple(final Tuple<K, V> tuple) {
    waitForInitializing();
    checkState(State.SENDING);
    return dataSender.sendTuple(tuple);
  }

  @Override
  public List<String> sendTuple(final List<Tuple<K, V>> tupleList) {
    waitForInitializing();
    checkState(State.SENDING);
    return dataSender.sendTuple(tupleList);
  }

  @Override
  public void sendTupleTo(final String receiverId, final Tuple<K, V> tuple) {
    waitForInitializing();
    checkState(State.SENDING);
    dataSender.sendTupleTo(receiverId, tuple);
  }

  @Override
  public void sendTupleTo(final String receiverId, final List<Tuple<K, V>> tupleList) {
    waitForInitializing();
    checkState(State.SENDING);
    dataSender.sendTupleTo(receiverId, tupleList);
  }

  private void waitForInitializing() {
    if (initialized.compareAndSet(false, true)) {
      controlMessageSender.sendToManager(PushShuffleCode.SENDER_INITIALIZED);
      synchronizer.waitOnLatch(PushShuffleCode.SHUFFLE_INITIALIZED);
      checkAndSetState(State.CREATED, State.SENDING);
    }
  }

  @Override
  public void complete() {
    LOG.log(Level.INFO, "Complete to send tuples");
    checkAndSetState(State.SENDING, State.WAITING);
    controlMessageSender.sendToManager(PushShuffleCode.SENDER_COMPLETED);
  }

  @Override
  public void waitForReceivers() {
    LOG.log(Level.INFO, "Wait for all receivers received");
    synchronizer.waitOnLatch(PushShuffleCode.ALL_RECEIVERS_RECEIVED);
    synchronizer.reopenLatch(PushShuffleCode.ALL_RECEIVERS_RECEIVED);
    checkAndSetState(State.WAITING, State.SENDING);
  }

  @Override
  public void completeAndWaitForReceivers() {
    complete();
    waitForReceivers();
  }

  public enum State {
    CREATED,
    SENDING,
    WAITING
  }

  private synchronized void checkState(final State expectedState) {
    if (currentState != expectedState) {
      throw new IllegalStateException("Expected state is " + expectedState + " but actual state is " + currentState);
    }
  }

  public static boolean isLegalTransition(final State from, final State to) {
    switch (from) {
    case CREATED:
      switch (to) {
      case SENDING:
        return true;
      default:
        return false;
      }
    case SENDING:
      switch (to) {
      case WAITING:
        return true;
      default:
        return false;
      }
    case WAITING:
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
}
