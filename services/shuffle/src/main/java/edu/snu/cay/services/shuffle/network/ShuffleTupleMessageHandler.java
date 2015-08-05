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

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Event handler for ShuffleTupleMessage.
 *
 * It routes messages to respective event handler registered with shuffle name.
 * If there is no registered event handler for some shuffle, the NullPointerException
 * will be thrown.
 */
public final class ShuffleTupleMessageHandler implements EventHandler<Message<ShuffleTupleMessage>> {

  private final Map<String, EventHandler> eventHandlerMap;

  @Inject
  private ShuffleTupleMessageHandler() {
    eventHandlerMap = new ConcurrentHashMap<>();
  }

  public <K, V> void registerMessageHandler(final String shuffleName,
                                            final EventHandler<Message<ShuffleTupleMessage<K, V>>> eventHandler) {
    if (!eventHandlerMap.containsKey(shuffleName)) {
      eventHandlerMap.put(shuffleName, eventHandler);
    }
  }

  @Override
  public void onNext(final Message<ShuffleTupleMessage> message) {
    final ShuffleTupleMessage tupleMessage = message.getData().iterator().next();
    eventHandlerMap.get(tupleMessage.getShuffleName()).onNext(message);
  }
}
