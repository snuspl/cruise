/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.pregel.graph.impl;

import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Manage message stores which are used to computation in one superstep.
 * Determine the incoming message store depending on the state of a worker.
 */
public class MessageManager<V> {

  private static final Logger LOG = Logger.getLogger(MessageManager.class.getName());

  private final GraphPartitioner graphPartitioner;

  /**
   * At current superstep, computation will use this to get messages sent.
   */
  private MessageStore<V> currMessageStore;

  /**
   * Message store which contains the message sent.
   */
  private MessageStore<V> nextMessageStore;

  /**
   * Message store which stores message in temporary until {@link #prepareForNextSuperstep()} is called.
   */
  private MessageStore<V> backUpMessageStore;

  public MessageManager(final GraphPartitioner graphPartitioner) {
    this.graphPartitioner = graphPartitioner;
    currMessageStore = new MessageStore<>(graphPartitioner);
    nextMessageStore = new MessageStore<>(graphPartitioner);
    backUpMessageStore = new MessageStore<>(graphPartitioner);
  }

  public void prepareForNextSuperstep() {
    currMessageStore = nextMessageStore;
    nextMessageStore = new MessageStore<>(graphPartitioner);
  }

  public void processBackUpMessageStore() {
    final Map<Integer, Set<V>> messageMap = backUpMessageStore.getAllMessages();
    messageMap.entrySet().forEach(entry -> entry.getValue().forEach(message ->
        nextMessageStore.writeMessage(entry.getKey(), message)));
    backUpMessageStore = new MessageStore<>(graphPartitioner);
  }

  public MessageStore<V> getCurrentMessageStore() {
    return currMessageStore;
  }

  public void writeMessage(final Integer vertexId, final V message) {
    nextMessageStore.writeMessage(vertexId, message);
  }
}









