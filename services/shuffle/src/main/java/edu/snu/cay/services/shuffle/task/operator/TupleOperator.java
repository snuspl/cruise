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
import edu.snu.cay.services.shuffle.strategy.ShuffleStrategy;

import java.util.List;

/**
 * Tuple operator for a specific shuffle.
 */
public interface TupleOperator<K, V> {

  /**
   * @return shuffle description
   */
  ShuffleDescription getShuffleDescription();

  /**
   * @return ShuffleStrategy instance for the operator
   */
  ShuffleStrategy<K> getShuffleStrategy();

  /**
   * Return selected receiver id list using the ShuffleStrategy among the receiver list
   *
   * @param key a key instance to select corresponding receivers
   * @return selected receiver id list
   */
  List<String> getSelectedReceiverIdList(K key);

}
