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
package edu.snu.cay.services.shuffle.task;

import edu.snu.cay.services.shuffle.description.ShuffleGroupDescription;

/**
 * The task side interface communicating with corresponding ShuffleGroupManager in driver
 * to control the shuffle group. The users can obtain tuple senders and receivers for
 * specific shuffle
 */
public interface ShuffleGroupClient {

  /**
   * Return the TupleReceiver for the shuffle. It throws RuntimeException if
   * the task is not a receiver for the shuffle.
   *
   * @param shuffleName the name of shuffle
   * @param <K> key type
   * @param <V> value type
   * @return tuple receiver
   */
  <K, V> TupleReceiver<K, V> getReceiver(String shuffleName);

  /**
   * Return the TupleSender for the shuffle. It throws RuntimeException if
   * the task is not a sender for the shuffle.
   *
   * @param shuffleName the name of shuffle
   * @param <K> key type
   * @param <V> value type
   * @return tuple sender
   */
  <K, V> TupleSender<K, V> getSender(String shuffleName);

  /**
   * @return description about shuffle group handled by the shuffle group client
   */
  ShuffleGroupDescription getShuffleGroupDescription();
}
