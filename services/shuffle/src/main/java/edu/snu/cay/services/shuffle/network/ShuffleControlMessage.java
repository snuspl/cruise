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
public final class ShuffleControlMessage extends ShuffleMessage<byte[]> {

  private final int code;

  /**
   * Construct a shuffle control message
   * @param code a code
   * @param shuffleName the name of shuffle
   * @param dataList a data list
   */
  public ShuffleControlMessage(final int code, final String shuffleName, final List<byte[]> dataList) {
    super(shuffleName, dataList);
    this.code = code;
  }

  public int getCode() {
    return code;
  }
}
