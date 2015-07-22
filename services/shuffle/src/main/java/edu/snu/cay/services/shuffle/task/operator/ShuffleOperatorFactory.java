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
package edu.snu.cay.services.shuffle.task.operator;

import edu.snu.cay.services.shuffle.description.ShuffleDescription;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Factory for creating tuple senders and receivers.
 */
@DefaultImplementation(ShuffleOperatorFactoryImpl.class)
public interface ShuffleOperatorFactory {

  /**
   * Create a shuffle receiver for the specified shuffle description and register a tuple codec for the operator.
   * It returns the cached receiver if the receiver was already created and throws runtime exception
   * if the task is not a receiver of the shuffle.
   *
   * @param shuffleDescription shuffle description
   * @param <K> key type
   * @param <V> value type
   * @return created or cached shuffle receiver
   */
  <K, V> ShuffleReceiver<K, V> newShuffleReceiver(ShuffleDescription shuffleDescription);

  /**
   * Create a shuffle sender for the specified shuffle description and register a tuple codec for the operator.
   * It returns the cached sender if the sender was already created and throws runtime exception
   * if the task is not a sender of the shuffle.
   *
   * @param shuffleDescription shuffle description
   * @param <K> key type
   * @param <V> value type
   * @return created or cached shuffle sender
   */
  <K, V> ShuffleSender<K, V> newShuffleSender(ShuffleDescription shuffleDescription);

}
