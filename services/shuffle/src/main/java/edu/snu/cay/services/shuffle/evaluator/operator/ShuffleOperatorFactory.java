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
package edu.snu.cay.services.shuffle.evaluator.operator;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Factory for creating tuple operators.
 */
@DefaultImplementation(ShuffleOperatorFactoryImpl.class)
public interface ShuffleOperatorFactory {

  /**
   * Create a shuffle receiver and register a tuple codec.
   * It returns the cached receiver if the receiver was already created and throws runtime exception
   * if the evaluator is not a receiver of the shuffle.
   *
   * @return created or cached shuffle receiver
   */
  <K, V> ShuffleReceiver<K, V> newShuffleReceiver();

  /**
   * Create a shuffle sender and register a tuple codec.
   * It returns the cached sender if the sender was already created and throws runtime exception
   * if the evaluator is not a sender of the shuffle.
   *
   * @return created or cached shuffle sender
   */
  <K, V> ShuffleSender<K, V> newShuffleSender();

}
