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
package edu.snu.cay.services.shuffle.evaluator;

import edu.snu.cay.services.shuffle.description.ShuffleDescription;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleSender;

/**
 * Evaluator side interface which communicates with corresponding ShuffleManager in driver,
 * and also provides shuffle operators to users.
 */
public interface Shuffle {

  /**
   * Return the ShuffleReceiver for the shuffle.
   *
   * It throws RuntimeException if the current evaluator is not a receiver for the shuffle.
   *
   * @return shuffle receiver
   */
  <K, V> ShuffleReceiver<K, V> getReceiver();

  /**
   * Return the ShuffleSender for the shuffle named shuffleName.
   *
   * It throws RuntimeException if the current evaluator is not a sender for the shuffle.
   *
   * @return shuffle sender
   */
  <K, V> ShuffleSender<K, V> getSender();

  /**
   * @return the shuffle description
   */
  ShuffleDescription getShuffleDescription();
}
