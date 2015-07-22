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

import edu.snu.cay.services.shuffle.description.ShuffleGroupDescription;
import edu.snu.cay.services.shuffle.task.ShuffleGroup;
import edu.snu.cay.services.shuffle.task.operator.ShuffleOperatorFactory;
import edu.snu.cay.services.shuffle.task.operator.ShuffleReceiver;
import edu.snu.cay.services.shuffle.task.operator.ShuffleSender;
import org.apache.reef.annotations.audience.TaskSide;

import javax.inject.Inject;

/**
 * Simple implementation of ShuffleGroup.
 *
 * The initial shuffle group description can never be changed. Users cannot add or remove more tasks
 * to shuffles and cannot change the key, value codecs and shuffling strategy of the certain shuffle
 * after the shuffle group is created.
 */
@TaskSide
public final class FixedShuffleGroup implements ShuffleGroup {

  private final ShuffleGroupDescription initialShuffleGroupDescription;
  private final ShuffleOperatorFactory operatorFactory;

  @Inject
  private FixedShuffleGroup(
      final ShuffleGroupDescription initialShuffleGroupDescription,
      final ShuffleOperatorFactory operatorFactory) {
    this.initialShuffleGroupDescription = initialShuffleGroupDescription;
    this.operatorFactory = operatorFactory;
  }

  @Override
  public <K, V> ShuffleReceiver<K, V> getReceiver(final String shuffleName) {
    return operatorFactory.newShuffleReceiver(initialShuffleGroupDescription.getShuffleDescription(shuffleName));
  }

  @Override
  public <K, V> ShuffleSender<K, V> getSender(final String shuffleName) {
    return operatorFactory.newShuffleSender(initialShuffleGroupDescription.getShuffleDescription(shuffleName));
  }

  @Override
  public ShuffleGroupDescription getShuffleGroupDescription() {
    return initialShuffleGroupDescription;
  }
}
