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
package edu.snu.cay.services.shuffle.evaluator.impl;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.driver.impl.StaticPushShuffleCode;
import edu.snu.cay.services.shuffle.evaluator.ESControlMessageSender;
import edu.snu.cay.services.shuffle.evaluator.ControlMessageSynchronizer;
import edu.snu.cay.services.shuffle.evaluator.Shuffle;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleOperatorFactory;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleSender;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO (#88) : Implement functionality.
/**
 * Simple implementation of Shuffle.
 *
 * The initial shuffle description can never be changed. Users cannot add or remove more tasks
 * to the shuffle and cannot change the key, value codecs and shuffling strategy after the Shuffle is created.
 */
@EvaluatorSide
public final class StaticPushShuffle<K, V> implements Shuffle<K, V> {

  private static final Logger LOG = Logger.getLogger(StaticPushShuffle.class.getName());

  private final ShuffleDescription shuffleDescription;
  private final ShuffleOperatorFactory<K, V> operatorFactory;
  private final ESControlMessageSender controlMessageSender;
  private final ControlMessageSynchronizer synchronizer;

  private final AtomicBoolean isSetupMessageSent;
  private final AtomicBoolean initialized;

  private final ControlMessageHandler controlMessageHandler;
  private final ControlLinkListener controlLinkListener;

  @Inject
  private StaticPushShuffle(
      final ShuffleDescription shuffleDescription,
      final ShuffleOperatorFactory<K, V> operatorFactory,
      final ESControlMessageSender controlMessageSender,
      final ControlMessageSynchronizer synchronizer) {
    this.shuffleDescription = shuffleDescription;
    this.operatorFactory = operatorFactory;
    this.synchronizer = synchronizer;

    this.isSetupMessageSent = new AtomicBoolean();
    this.initialized = new AtomicBoolean();
    this.controlMessageSender = controlMessageSender;

    this.controlMessageHandler = new ControlMessageHandler();
    this.controlLinkListener = new ControlLinkListener();
  }

  /**
   * @return the ShuffleReceiver of the Shuffle
   */
  @Override
  public <T extends ShuffleReceiver<K, V>> T getReceiver() {
    sendSetupMessage();
    return operatorFactory.newShuffleReceiver();
  }

  /**
   * @return the ShuffleSender of the Shuffle
   */
  @Override
  public <T extends ShuffleSender<K, V>> T getSender() {
    sendSetupMessage();
    return operatorFactory.newShuffleSender();
  }

  /**
   * Send a setup message to driver. This should not be called in the constructor
   * since the initialization can be finished before users register their shuffle message handler.
   */
  private void sendSetupMessage() {
    if (isSetupMessageSent.compareAndSet(false, true)) {
      controlMessageSender.sendToManager(StaticPushShuffleCode.END_POINT_INITIALIZED);
    }
  }

  /**
   * @param code a code for expecting ShuffleControlMessage
   * @return the ShuffleControlMessage
   */
  @Override
  public Optional<ShuffleControlMessage> waitForControlMessage(final int code) {
    return synchronizer.waitOnLatch(code);
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
    public void onNext(final Message<ShuffleControlMessage> networkControlMessage) {
      final ShuffleControlMessage controlMessage = networkControlMessage.getData().iterator().next();
      if (controlMessage.getCode() == StaticPushShuffleCode.SHUFFLE_INITIALIZED) {
        if (initialized.compareAndSet(false, true)) {
          synchronizer.closeLatch(controlMessage);
        }
      }
    }
  }

  private final class ControlLinkListener implements LinkListener<Message<ShuffleControlMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleControlMessage> networkControlMessage) {
      LOG.log(Level.FINE, "ShuffleControlMessage was successfully sent : {0}", networkControlMessage);
    }

    @Override
    public void onException(
        final Throwable throwable,
        final SocketAddress socketAddress,
        final Message<ShuffleControlMessage> networkControlMessage) {
      LOG.log(Level.WARNING, "An exception occurred while sending ShuffleControlMessage to driver. cause : {0}, " +
          "socket address : {1}, message : {2}", new Object[]{throwable, socketAddress, networkControlMessage});
      throw new RuntimeException("An exception occurred while sending ShuffleControlMessage to driver", throwable);
      // TODO (#67) : failure handling.
    }
  }
}
