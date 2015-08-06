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
import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Link listener for ShuffleMessage.
 *
 * It routes exception or success callback to a respective link lister registered with shuffle name.
 * If there is no registered link listener for some ShuffleMessage, the log message
 * will be printed.
 */
abstract class ShuffleLinkListener<K extends ShuffleMessage> implements LinkListener<Message<K>> {

  private static final Logger LOG = Logger.getLogger(ShuffleLinkListener.class.getName());

  private final Map<String, LinkListener> linkListenerMap;

  ShuffleLinkListener() {
    this.linkListenerMap = new ConcurrentHashMap<>();
  }

  public void registerLinkListener(final String shuffleName,
                                   final LinkListener<Message<K>> linkListener) {
    if (!linkListenerMap.containsKey(shuffleName)) {
      linkListenerMap.put(shuffleName, linkListener);
    }
  }

  @Override
  public void onSuccess(final Message<K> message) {
    final ShuffleMessage shuffleMessage = message.getData().iterator().next();
    final LinkListener<Message<K>> linkListener = linkListenerMap.get(shuffleMessage.getShuffleName());
    if (linkListener != null) {
      linkListener.onSuccess(message);
    } else {
      LOG.log(Level.INFO, "There is no registered link listener for {0}. A message was successfully sent {1}.",
          new Object[]{shuffleMessage.getShuffleName(), message});
    }
  }

  @Override
  public void onException(
      final Throwable cause, final SocketAddress remoteAddress, final Message<K> message) {
    final ShuffleMessage shuffleMessage = message.getData().iterator().next();
    final LinkListener<Message<K>> linkListener = linkListenerMap.get(shuffleMessage.getShuffleName());
    if (linkListener != null) {
      linkListener.onException(cause, remoteAddress, message);
    } else {
      LOG.log(Level.INFO, "There is no registered link listener for {0}. An exception occurred while sending {1}.",
          new Object[]{shuffleMessage.getShuffleName(), message});
    }
  }
}
