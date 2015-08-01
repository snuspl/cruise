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
 * Shuffle Tuple message containing tuples and the shuffle name.
 */
public final class ShuffleTupleMessage<K, V> {

  private final String shuffleName;
  private final List<Tuple<K, V>> tupleList;

  /**
   * Construct a shuffle message tuple
   *
   * @param shuffleName the name of shuffle
   * @param tupleList a tuple list
   */
  public ShuffleTupleMessage(
      final String shuffleName,
      final List<Tuple<K, V>> tupleList) {
    this.shuffleName = shuffleName;
    this.tupleList = tupleList;
  }

  public String getShuffleName() {
    return shuffleName;
  }

  public int size() {
    if (tupleList == null) {
      return 0;
    }

    return tupleList.size();
  }

  public Tuple<K, V> get(final int index) {
    return tupleList.get(index);
  }
}
