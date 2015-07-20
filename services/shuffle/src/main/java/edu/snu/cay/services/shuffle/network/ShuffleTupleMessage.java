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
package edu.snu.cay.services.shuffle.network;

import org.apache.reef.io.Tuple;

import java.util.List;

/**
 * Shuffle Tuple message which is transferred by NetworkConnectionService
 */
public final class ShuffleTupleMessage<K, V> {

  private final String shuffleGroupName;
  private final String shuffleName;
  private final List<Tuple<K, V>> tuples;

  /**
   * Construct a shuffle message tuple
   *
   * @param shuffleGroupName the name of shuffle group
   * @param shuffleName the name of shuffle
   * @param tupleList a tuple list
   */
  public ShuffleTupleMessage(
      final String shuffleGroupName,
      final String shuffleName,
      final List<Tuple<K, V>> tupleList) {
    this.shuffleGroupName = shuffleGroupName;
    this.shuffleName = shuffleName;
    this.tuples = tupleList;
  }

  /**
   * @return shuffle group name
   */
  public String getShuffleGroupName() {
    return shuffleGroupName;
  }

  /**
   * @return shuffle name
   */
  public String getShuffleName() {
    return shuffleName;
  }

  /**
   * @return the size of tuples included in the tuple message
   */
  public int size() {
    if (tuples == null) {
      return 0;
    }

    return tuples.size();
  }

  /**
   * Return a tuple at the specified index
   *
   * @param index an index of tuple
   * @return tuple at index
   */
  public Tuple<K, V> get(final int index) {
    return tuples.get(index);
  }
}
