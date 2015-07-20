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

import edu.snu.cay.services.shuffle.description.ShuffleDescription;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Factory for creating tuple sender and receiver
 */
@DefaultImplementation(TupleOperatorFactoryImpl.class)
public interface TupleOperatorFactory {

  /**
   * Create a tuple receiver for specified shuffle description and register tuple codec for the operator.
   * It returns cached tuple receiver if the tuple receiver was already created and throws runtime exception
   * if the task is not a receiver for the shuffle.
   *
   * @param shuffleDescription shuffle description
   * @param <K> key type
   * @param <V> value type
   * @return created or cached tuple receiver
   */
  <K, V> TupleReceiver<K, V> newTupleReceiver(ShuffleDescription shuffleDescription);

  /**
   * Create a tuple sender for specified shuffle description and register tuple codec for the operator.
   * It returns cached tuple sender if the tuple sender was already created and throws runtime exception
   * if the task is not a sender for the shuffle.
   *
   * @param shuffleDescription shuffle description
   * @param <K> key type
   * @param <V> value type
   * @return created or cached tuple sender
   */
  <K, V> TupleSender<K, V> newTupleSender(ShuffleDescription shuffleDescription);

}
