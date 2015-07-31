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
package edu.snu.cay.services.shuffle.evaluator;

import edu.snu.cay.services.shuffle.description.ShuffleDescription;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleOperatorFactory;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleSender;

import javax.inject.Inject;

/**
 * Simple implementation of Shuffle.
 *
 * The initial shuffle description can never be changed. Users cannot add or remove more tasks
 * to the shuffle and cannot change the key, value codecs and shuffling strategy after the Shuffle is created.
 */
public final class StaticShuffle implements Shuffle {

  private final ShuffleDescription shuffleDescription;
  private final ShuffleOperatorFactory operatorFactory;

  @Inject
  private StaticShuffle(
      final ShuffleDescription shuffleGroupDescription,
      final ShuffleOperatorFactory operatorFactory) {
    this.shuffleDescription = shuffleGroupDescription;
    this.operatorFactory = operatorFactory;
  }

  @Override
  public <K, V> ShuffleReceiver<K, V> getReceiver() {
    return operatorFactory.newShuffleReceiver();
  }

  @Override
  public <K, V> ShuffleSender<K, V> getSender() {
    return operatorFactory.newShuffleSender();
  }

  @Override
  public ShuffleDescription getShuffleDescription() {
    return shuffleDescription;
  }
}
