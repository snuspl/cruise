/**
 * Copyright (C) 2014 Seoul National University
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

package org.apache.reef.elastic.memory.utils;

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Event Handler for custom Network Service Wrapper
 */
public final class NSWrapperMessageHandler implements EventHandler<Message<String>> {

  private EventHandler<Message<String>> taskReceiveHandler;

  @Inject
  public NSWrapperMessageHandler() {
    taskReceiveHandler = null;
  }

  @Override
  public void onNext(Message<String> msg) {
    if (taskReceiveHandler != null) {
      // Passes message to a handler inside the Task context
      taskReceiveHandler.onNext(msg);
    }
  }

  public void registerReceiverHandler(EventHandler<Message<String>> receiveHandler) {
    this.taskReceiveHandler = receiveHandler;
  }
}
