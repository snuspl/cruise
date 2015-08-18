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
import edu.snu.cay.services.shuffle.driver.ShuffleManager;
import edu.snu.cay.services.shuffle.evaluator.impl.StaticPushShuffle;
import edu.snu.cay.services.shuffle.evaluator.operator.impl.PushShuffleReceiverImpl;
import edu.snu.cay.services.shuffle.evaluator.operator.impl.PushShuffleSenderImpl;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.services.shuffle.utils.ShuffleDescriptionSerializer;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple implementation of ShuffleManager.
 *
 * The initial shuffle description can never be changed. Users cannot add or remove more tasks
 * to the shuffle and cannot change the key, value codecs and shuffling strategy after the manager is created.
 *
 * The manager broadcasts a SHUFFLE_INITIALIZED message to all end points in the shuffle
 * when all the end points are initialized.
 */
@DriverSide
public final class StaticPushShuffleManager implements ShuffleManager {

  private static final Logger LOG = Logger.getLogger(StaticPushShuffleManager.class.getName());

  private final ShuffleDescription shuffleDescription;
  private final ShuffleDescriptionSerializer descriptionSerializer;
  private final StaticPushShuffleRepresenters representers;

  private final ControlMessageHandler controlMessageHandler;
  private final ControlLinkListener controlLinkListener;

  @Inject
  private StaticPushShuffleManager(
      final ShuffleDescription shuffleDescription,
      final ShuffleDescriptionSerializer descriptionSerializer,
      final StaticPushShuffleRepresenters representers) {
    this.shuffleDescription = shuffleDescription;
    this.descriptionSerializer = descriptionSerializer;
    this.representers = representers;

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
      representers.onControlMessage(message.getSrcId().toString(), controlMessage);
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
}
