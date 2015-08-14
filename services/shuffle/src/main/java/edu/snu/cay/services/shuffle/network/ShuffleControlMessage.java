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
 * Shuffle control message containing byte data list and the shuffle name.
 */
public final class ShuffleControlMessage extends ShuffleMessage<String> {

  private final int code;

  /**
   * Construct a shuffle control message.
   *
   * @param code a code
   * @param shuffleName the name of shuffle
   * @param endPointIdList a list of end point id that are related with the control message
   */
  public ShuffleControlMessage(final int code, final String shuffleName, final List<String> endPointIdList) {
    super(shuffleName, endPointIdList);
    this.code = code;
  }

  /**
   * Construct a shuffle control message.
   *
   * @param code a code
   * @param shuffleName the name of shuffle
   */
  public ShuffleControlMessage(final int code, final String shuffleName) {
    super(shuffleName, null);
    this.code = code;
  }

  public int getCode() {
    return code;
  }
}
