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
import edu.snu.cay.services.shuffle.evaluator.impl.BasicShuffle;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.services.shuffle.utils.ShuffleDescriptionSerializer;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO (#82) : This class will be removed and StaticPushShuffleManager will be added
// as a basic implementation of ShuffleManager.
/**
 * Simple implementation of ShuffleManager.
 *
 * The initial shuffle description can never be changed. Users cannot add or remove more tasks
 * to the shuffle and cannot change the key, value codecs and shuffling strategy after the manager is created.
 *
 * The manager broadcasts a MANAGER_SETUP message to all end points in the shuffle
 * when all the end points are initialized.
 */
@DriverSide
public final class BasicShuffleManager implements ShuffleManager {

  private static final Logger LOG = Logger.getLogger(BasicShuffleManager.class.getName());

  private final ShuffleDescription shuffleDescription;
  private final ShuffleDescriptionSerializer descriptionSerializer;
  private final DSControlMessageSender controlMessageSender;
  private final Set<String> endPointIdSet;
  private final AtomicInteger setupEndPointCount;

  private final ControlMessageHandler controlMessageHandler;
  private final ControlLinkListener controlLinkListener;

  @Inject
  private BasicShuffleManager(
      final ShuffleDescription shuffleDescription,
      final ShuffleDescriptionSerializer descriptionSerializer,
      final DSControlMessageSender controlMessageSender) {
    this.shuffleDescription = shuffleDescription;
    this.descriptionSerializer = descriptionSerializer;
    this.controlMessageSender = controlMessageSender;
    this.endPointIdSet = new HashSet<>();
    this.endPointIdSet.addAll(shuffleDescription.getReceiverIdList());
    this.endPointIdSet.addAll(shuffleDescription.getSenderIdList());
    this.setupEndPointCount = new AtomicInteger(this.endPointIdSet.size());

    this.controlMessageHandler = new ControlMessageHandler();
    this.controlLinkListener = new ControlLinkListener();
  }

  /**
   * @param endPointId end point id
   * @return Serialized shuffle description for the endPointId
   */
  @Override
  public Optional<Configuration> getShuffleConfiguration(final String endPointId) {
    return descriptionSerializer.serialize(BasicShuffle.class, shuffleDescription, endPointId);
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

  private void broadcastSetupMessage() {
    try {
      for (final String endPointId : endPointIdSet) {
        controlMessageSender.send(endPointId, BasicShuffleCode.MANAGER_SETUP);
      }
    } catch (final NetworkException e) {
      // TODO (#67) : failure handling
      throw new RuntimeException(e);
    }
  }

  private final class ControlMessageHandler implements EventHandler<Message<ShuffleControlMessage>> {

    @Override
    public void onNext(final Message<ShuffleControlMessage> networkControlMessage) {
      final ShuffleControlMessage controlMessage = networkControlMessage.getData().iterator().next();
      if (controlMessage.getCode() == BasicShuffleCode.SHUFFLE_SETUP) {
        if (setupEndPointCount.decrementAndGet() == 0) {
          // TODO (#82) : This redundant sleep will be removed and StaticPushShuffleManager will be added.
          try {
            // Wait for all tasks register their EventHandler.
            Thread.sleep(2000);
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
          broadcastSetupMessage();
        }
      }
    }
  }

  private final class ControlLinkListener implements LinkListener<Message<ShuffleControlMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleControlMessage> networkControlMessage) {
      LOG.log(Level.FINE, "A ShuffleControlMessage was successfully sent : {0}", networkControlMessage);
    }

    @Override
    public void onException(
        final Throwable cause,
        final SocketAddress socketAddress,
        final Message<ShuffleControlMessage> networkControlMessage) {
      LOG.log(Level.WARNING, "An exception occurred while sending a ShuffleControlMessage. cause : {0}," +
          " socket address : {1}, message : {2}", new Object[]{cause, socketAddress, networkControlMessage});
      // TODO (#67) : failure handling.
    }
  }
}
