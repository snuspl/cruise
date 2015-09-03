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
package edu.snu.cay.services.shuffle.driver;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleSender;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import edu.snu.cay.services.shuffle.evaluator.Shuffle;
import edu.snu.cay.services.shuffle.strategy.ShuffleStrategy;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;

/**
 * Serialize configurations for Shuffle in evaluators using Tang.
 */
public final class ShuffleConfigurationSerializer {

  private final ConfigurationSerializer confSerializer;

  @Inject
  private ShuffleConfigurationSerializer(final ConfigurationSerializer confSerializer) {
    this.confSerializer = confSerializer;
  }

  /**
   * Return serialized Configuration for endPointId with certain types of shuffle, sender and receiver.
   *
   * @param shuffleClass a type of Shuffle
   * @param senderClass a type of ShuffleSender
   * @param receiverClass a type of ShuffleReceiver
   * @param shuffleDescription a shuffle description to serialize
   * @param endPointId an end point identifier
   * @return serialized configuration
   */
  public Configuration serialize(
      final Class<? extends Shuffle> shuffleClass,
      final Class<? extends ShuffleSender> senderClass,
      final Class<? extends ShuffleReceiver> receiverClass,
      final ShuffleDescription shuffleDescription,
      final String endPointId) {
    final Configuration shuffleConfiguration = createShuffleConfiguration(
        shuffleClass,
        senderClass,
        receiverClass,
        shuffleDescription,
        endPointId
    );

    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(
            ShuffleParameters.SerializedShuffleSet.class, confSerializer.toString(shuffleConfiguration))
        .build();
  }

  private Configuration createShuffleConfiguration(
      final Class<? extends Shuffle> shuffleClass,
      final Class<? extends ShuffleSender> senderClass,
      final Class<? extends ShuffleReceiver> receiverClass,
      final ShuffleDescription shuffleDescription,
      final String endPointId) {
    final String shuffleName = shuffleDescription.getShuffleName();
    final boolean isSender = shuffleDescription.getSenderIdList().contains(endPointId);
    final boolean isReceiver = shuffleDescription.getReceiverIdList().contains(endPointId);
    if (!isSender && !isReceiver) {
      throw new RuntimeException(endPointId + " is not a sender or receiver of " + shuffleName);
    }

    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Shuffle.class, shuffleClass)
        .bindImplementation(ShuffleStrategy.class, shuffleDescription.getShuffleStrategyClass())
        .bindNamedParameter(ShuffleParameters.ShuffleName.class, shuffleName)
        .bindNamedParameter(ShuffleParameters.EndPointId.class, endPointId)
        .bindNamedParameter(ShuffleParameters.TupleKeyCodec.class, shuffleDescription.getKeyCodecClass())
        .bindNamedParameter(ShuffleParameters.TupleValueCodec.class, shuffleDescription.getValueCodecClass())
        .bindNamedParameter(
            ShuffleParameters.ShuffleKeyCodecClassName.class, shuffleDescription.getKeyCodecClass().getName())
        .bindNamedParameter(
            ShuffleParameters.ShuffleValueCodecClassName.class, shuffleDescription.getValueCodecClass().getName())
        .bindNamedParameter(
            ShuffleParameters.ShuffleStrategyClassName.class, shuffleDescription.getShuffleStrategyClass().getName());

    if (isSender) {
      confBuilder.bindImplementation(ShuffleSender.class, senderClass);
    }

    if (isReceiver) {
      confBuilder.bindImplementation(ShuffleReceiver.class, receiverClass);
    }

    for (final String senderId : shuffleDescription.getSenderIdList()) {
      confBuilder.bindSetEntry(ShuffleParameters.ShuffleSenderIdSet.class, senderId);
    }

    for (final String receiverId : shuffleDescription.getReceiverIdList()) {
      confBuilder.bindSetEntry(ShuffleParameters.ShuffleReceiverIdSet.class, receiverId);
    }

    return confBuilder.build();
  }
}
