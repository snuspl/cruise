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
package edu.snu.cay.services.shuffle.network;

import java.util.List;

/**
 *  Shuffle message containing data list and the shuffle name.
 */
abstract class ShuffleMessage<T> {
  private final String shuffleName;
  private final List<T> dataList;

  /**
   * Construct a shuffle message.
   *
   * @param shuffleName the name of shuffle
   * @param dataList a data list
   */
  public ShuffleMessage(final String shuffleName, final List<T> dataList) {
    this.shuffleName = shuffleName;
    this.dataList = dataList;
  }

  public String getShuffleName() {
    return shuffleName;
  }

  public int size() {
    if (dataList == null) {
      return 0;
    }

    return dataList.size();
  }

  public T get(final int index) {
    return dataList.get(index);
  }
}
