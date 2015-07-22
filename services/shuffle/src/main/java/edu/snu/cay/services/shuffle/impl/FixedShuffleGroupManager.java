/**
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
package edu.snu.cay.services.shuffle.impl;

import edu.snu.cay.services.shuffle.description.ShuffleDescription;
import edu.snu.cay.services.shuffle.description.ShuffleGroupDescription;
import edu.snu.cay.services.shuffle.driver.ShuffleGroupManager;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import edu.snu.cay.services.shuffle.task.ShuffleGroup;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.List;

/**
 * Simple implementation of ShuffleGroupManager.
 *
 * The initial shuffle group description can never be changed. Users cannot add or remove more tasks
 * to shuffles and cannot change the key, value codecs and shuffling strategy of the certain shuffle
 * after the manager is created.
 */
@DriverSide
public final class FixedShuffleGroupManager implements ShuffleGroupManager {

  private final ShuffleGroupDescription initialShuffleGroupDescription;
  private final ConfigurationSerializer confSerializer;

  @Inject
  private FixedShuffleGroupManager(
      final ShuffleGroupDescription initialShuffleGroupDescription,
      final ConfigurationSerializer confSerializer) {
    this.initialShuffleGroupDescription = initialShuffleGroupDescription;
    this.confSerializer = confSerializer;
  }

  @Override
  public Configuration getShuffleGroupConfigurationForTask(final String taskId) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindNamedParameter(ShuffleParameters.ShuffleGroupName.class,
        initialShuffleGroupDescription.getShuffleGroupName());

    boolean isTaskIncludedSomeShuffle = false;

    for (final String shuffleName : initialShuffleGroupDescription.getShuffleNameList()) {
      final ShuffleDescription shuffleDescription = initialShuffleGroupDescription.getShuffleDescription(shuffleName);
      final List<String> senderIdList = shuffleDescription.getSenderIdList();
      final List<String> receiverIdList = shuffleDescription.getReceiverIdList();
      if (senderIdList.contains(taskId) || receiverIdList.contains(taskId)) {
        isTaskIncludedSomeShuffle = true;
        bindShuffleDescription(confBuilder, shuffleDescription);
      }
    }

    if (!isTaskIncludedSomeShuffle) {
      return null;
    }
    return confBuilder.build();
  }

  private void bindShuffleDescription(
      final JavaConfigurationBuilder confBuilder,
      final ShuffleDescription shuffleDescription) {
    final Configuration shuffleConfiguration = serializeShuffleDescriptionWithSenderReceiver(shuffleDescription);

    confBuilder.bindSetEntry(
        ShuffleParameters.SerializedShuffleSet.class, confSerializer.toString(shuffleConfiguration));
  }

  private Configuration serializeShuffleDescriptionWithSenderReceiver(final ShuffleDescription shuffleDescription) {

    final String shuffleName = shuffleDescription.getShuffleName();

    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindNamedParameter(ShuffleParameters.ShuffleName.class, shuffleName);
    confBuilder.bindNamedParameter(
        ShuffleParameters.ShuffleStrategyClassName.class, shuffleDescription.getShuffleStrategyClass().getName());
    confBuilder.bindNamedParameter(
        ShuffleParameters.ShuffleKeyCodecClassName.class, shuffleDescription.getKeyCodecClass().getName());
    confBuilder.bindNamedParameter(
        ShuffleParameters.ShuffleValueCodecClassName.class, shuffleDescription.getValueCodecClass().getName());

    for (final String senderId : shuffleDescription.getSenderIdList()) {
      confBuilder.bindSetEntry(ShuffleParameters.ShuffleSenderIdSet.class, senderId);
    }

    for (final String receiverId : shuffleDescription.getReceiverIdList()) {
      confBuilder.bindSetEntry(ShuffleParameters.ShuffleReceiverIdSet.class, receiverId);
    }

    return confBuilder.build();
  }

  @Override
  public Class<? extends ShuffleGroup> getShuffleGroupClass() {
    return FixedShuffleGroup.class;
  }

  @Override
  public ShuffleGroupDescription getShuffleGroupDescription() {
    return initialShuffleGroupDescription;
  }
}
