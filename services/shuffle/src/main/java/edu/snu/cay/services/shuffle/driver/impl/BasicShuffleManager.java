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
import edu.snu.cay.services.shuffle.evaluator.impl.BasicShuffle;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.services.shuffle.utils.ShuffleDescriptionSerializer;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.net.SocketAddress;

/**
 * Simple implementation of ShuffleManager.
 *
 * The initial shuffle description can never be changed. Users cannot add or remove more tasks
 * to the shuffle and cannot change the key, value codecs and shuffling strategy after the manager is created.
 */
@DriverSide
public final class BasicShuffleManager implements ShuffleManager {

  private final ShuffleDescription shuffleDescription;
  private final ShuffleDescriptionSerializer descriptionSerializer;

  @Inject
  private BasicShuffleManager(
      final ShuffleDescription shuffleDescription,
      final ShuffleDescriptionSerializer descriptionSerializer) {
    this.shuffleDescription = shuffleDescription;
    this.descriptionSerializer = descriptionSerializer;
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
  public void onNext(final Message<ShuffleControlMessage> shuffleControlMessage) {

  }

  @Override
  public void onSuccess(final Message<ShuffleControlMessage> shuffleControlMessage) {

  }

  @Override
  public void onException(
      final Throwable throwable,
      final SocketAddress socketAddress,
      final Message<ShuffleControlMessage> shuffleControlMessage) {

  }
}
