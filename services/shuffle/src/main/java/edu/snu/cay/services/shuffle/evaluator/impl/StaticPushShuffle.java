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
import edu.snu.cay.services.shuffle.driver.impl.PushShuffleCode;
import edu.snu.cay.services.shuffle.evaluator.ControlMessageSynchronizer;
import edu.snu.cay.services.shuffle.evaluator.Shuffle;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleOperatorFactory;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleSender;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Shuffle implementation for static push-based shuffling.
 *
 * The initial shuffle description can never be changed. Users cannot add or remove more tasks
 * to the shuffle and cannot change the key, value codecs and shuffling strategy after the Shuffle is created.
 */
@EvaluatorSide
public final class StaticPushShuffle<K, V> implements Shuffle<K, V> {

  private static final Logger LOG = Logger.getLogger(StaticPushShuffle.class.getName());

  private final ShuffleDescription shuffleDescription;
  private final ShuffleOperatorFactory<K, V> operatorFactory;
  private final ControlMessageSynchronizer synchronizer;

  private final AtomicBoolean initialized;

  private final ControlMessageHandler controlMessageHandler;
  private final ControlLinkListener controlLinkListener;

  private ShuffleReceiver<K, V> shuffleReceiver;
  private ShuffleSender<K, V> shuffleSender;

  @Inject
  private StaticPushShuffle(
      final ShuffleDescription shuffleDescription,
      final ShuffleOperatorFactory<K, V> operatorFactory,
      final ControlMessageSynchronizer synchronizer) {
    this.shuffleDescription = shuffleDescription;
    this.operatorFactory = operatorFactory;
    this.synchronizer = synchronizer;

    this.initialized = new AtomicBoolean();

    this.controlMessageHandler = new ControlMessageHandler();
    this.controlLinkListener = new ControlLinkListener();
  }

  /**
   * @return the ShuffleReceiver of the Shuffle
   */
  @Override
  public <T extends ShuffleReceiver<K, V>> T getReceiver() {
    shuffleReceiver = operatorFactory.newShuffleReceiver();
    return (T)shuffleReceiver;
  }

  /**
   * @return the ShuffleSender of the Shuffle
   */
  @Override
  public <T extends ShuffleSender<K, V>> T getSender() {
    shuffleSender = operatorFactory.newShuffleSender();
    return (T) shuffleSender;
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
      switch (controlMessage.getCode()) {
      case PushShuffleCode.SHUFFLE_INITIALIZED:
        if (initialized.compareAndSet(false, true)) {
          synchronizer.closeLatch(controlMessage);
        }
        break;

      case PushShuffleCode.ALL_RECEIVERS_RECEIVED:
        LOG.log(Level.INFO, "All receivers received tuples from senders");
        synchronizer.closeLatch(controlMessage);
        break;

      case PushShuffleCode.ALL_SENDERS_COMPLETED:
        LOG.log(Level.INFO, "All senders are completed to send tuples");
        synchronizer.closeLatch(controlMessage);
        break;

      default:
        throw new RuntimeException("Illegal code[ " + controlMessage.getCode() + " ] from " + message.getSrcId());
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
