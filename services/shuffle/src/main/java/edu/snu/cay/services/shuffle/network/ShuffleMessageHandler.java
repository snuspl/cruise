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

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Event handler for ShuffleMessage.
 *
 * It routes messages to a respective event handler registered with shuffle name.
 * If there is no registered event handler for some shuffle, the NullPointerException
 * will be thrown.
 */
abstract class ShuffleMessageHandler<K extends ShuffleMessage> implements EventHandler<Message<K>> {

  private final Map<String, EventHandler> eventHandlerMap;

  ShuffleMessageHandler() {
    eventHandlerMap = new ConcurrentHashMap<>();
  }

  public void registerMessageHandler(final String shuffleName,
                                     final EventHandler<Message<K>> eventHandler) {
    if (!eventHandlerMap.containsKey(shuffleName)) {
      eventHandlerMap.put(shuffleName, eventHandler);
    }
  }

  @Override
  public void onNext(final Message<K> message) {
    final ShuffleMessage shuffleMessage = message.getData().iterator().next();
    eventHandlerMap.get(shuffleMessage.getShuffleName()).onNext(message);
  }
}
