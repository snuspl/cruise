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
 * Shuffle control message containing end point id list and certain code.
 */
public final class ShuffleControlMessage {

  private final int code;
  private final List<String> endPointIdList;

  /**
   * Construct a shuffle control message.
   *
   * @param code a code
   * @param endPointIdList a list of end point id that are related with the control message
   */
  public ShuffleControlMessage(final int code, final List<String> endPointIdList) {
    this.code = code;
    this.endPointIdList = endPointIdList;
  }

  /**
   * Construct a shuffle control message.
   *
   * @param code a code
   */
  public ShuffleControlMessage(final int code) {
    this.code = code;
    this.endPointIdList = null;
  }

  /**
   * @return the code
   */
  public int getCode() {
    return code;
  }

  /**
   * @return the size
   */
  public int size() {
    if (endPointIdList == null) {
      return 0;
    }

    return endPointIdList.size();
  }

  /**
   * Return end point id at index.
   *
   * @param index an index
   * @return the end point id
   */
  public String get(final int index) {
    return endPointIdList.get(index);
  }
}
