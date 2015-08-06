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
import edu.snu.cay.services.shuffle.evaluator.Shuffle;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleOperatorFactory;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleSender;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple implementation of Shuffle.
 *
 * The initial shuffle description can never be changed. Users cannot add or remove more tasks
 * to the shuffle and cannot change the key, value codecs and shuffling strategy after the Shuffle is created.
 */
@EvaluatorSide
public final class BasicShuffle<K, V> implements Shuffle<K, V> {

  private static final Logger LOG = Logger.getLogger(BasicShuffle.class.getName());

  private final ShuffleDescription shuffleDescription;
  private final ShuffleOperatorFactory<K, V> operatorFactory;

  @Inject
  private BasicShuffle(
      final ShuffleDescription shuffleDescription,
      final ShuffleOperatorFactory<K, V> operatorFactory) {
    this.shuffleDescription = shuffleDescription;
    this.operatorFactory = operatorFactory;
  }

  /**
   * @return the ShuffleReceiver of the Shuffle
   */
  @Override
  public <T extends ShuffleReceiver<K, V>> T getReceiver() {
    return operatorFactory.newShuffleReceiver();
  }

  /**
   * @return the ShuffleSender of the Shuffle
   */
  @Override
  public <T extends ShuffleSender<K, V>> T getSender() {
    return operatorFactory.newShuffleSender();
  }

  /**
   * @param code a code for expecting ShuffleControlMessage
   * @return the ShuffleControlMessage
   */
  @Override
  public ShuffleControlMessage waitForControlMessage(final int code) {
    return null;
  }

  /**
   * @return the initial shuffle description
   */
  @Override
  public ShuffleDescription getShuffleDescription() {
    return shuffleDescription;
  }

  @Override
  public void onNext(final Message<ShuffleControlMessage> shuffleControlMessage) {

  }

  @Override
  public void onSuccess(final Message<ShuffleControlMessage> shuffleControlMessage) {
    LOG.log(Level.FINE, "ShuffleControlMessage was successfully sent : {0}", shuffleControlMessage);
  }

  @Override
  public void onException(
      final Throwable throwable,
      final SocketAddress socketAddress,
      final Message<ShuffleControlMessage> shuffleControlMessage) {
    LOG.log(Level.WARNING, "An exception occurred while sending ShuffleControlMessage to driver. cause : {0}, " +
        "socket address : {1}, message : {2}", new Object[]{throwable, socketAddress, shuffleControlMessage});
  }
}
