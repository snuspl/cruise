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
package edu.snu.cay.services.em.utils;

import org.apache.reef.io.network.Message;

public final class SingleMessageExtractor {

  /**
   * Should not be instantiated.
   */
  private SingleMessageExtractor() {
  }

  public static <T> T extract(final Message<T> msg) {
    boolean foundMessage = false;
    T singleMsg = null;

    for (final T innerMsg : msg.getData()) {
      if (foundMessage) {
        throw new RuntimeException("More than one message was sent.");
      }

      foundMessage = true;
      singleMsg = innerMsg;
    }

    if (!foundMessage) {
      throw new RuntimeException("No message contents were found.");
    }

    return singleMsg;
  }
}
