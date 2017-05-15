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

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

/**
 * Strucure that stores messages sent to vertex.
 *
 * @param <MV> message value
 */
public final class MessageStore<MV> {

  private static final Logger LOG = Logger.getLogger(MessageStore.class.getName());

  /**
   * the map which key is partition id and value is vertex id to messages map.
   */
  private final ConcurrentMap<Integer, ConcurrentMap<Integer, Set<MV>>> messageMap;
  private final GraphPartitioner graphPartitioner;

  public MessageStore(final GraphPartitioner graphPartitioner) {
    this.messageMap = new ConcurrentHashMap<>();
    this.graphPartitioner = graphPartitioner;
  }

  /**
   * Get messages for a vertex.
   *
   * @param vertexId vertex id for which we want to get messages
   * @return Iterable of messages for a vertex id
   */
  public Iterable<MV> getVertexMessages(final Integer vertexId) {
    final int partitionIdx = graphPartitioner.getPartitionIdx(vertexId);
    if (!messageMap.containsKey(partitionIdx) || !messageMap.get(partitionIdx).containsKey(vertexId)) {
      return Collections.emptySet();
    }
    return messageMap.get(partitionIdx).get(vertexId);
  }

  /**
   * Write a message for a particular vertex.
   *
   * @param vertexId vertex id for a target vertex
   * @param value a message value to be send
   */
  public void writeMessage(final Integer vertexId, final MV value) {
    final int partitionIdx = graphPartitioner.getPartitionIdx(vertexId);
    messageMap.putIfAbsent(partitionIdx, Maps.newConcurrentMap());
    messageMap.get(partitionIdx).putIfAbsent(vertexId, new HashSet<>());
    messageMap.get(partitionIdx).get(vertexId).add(value);
  }

  /**
   * Get all messages in this message store.
   *
   * @return map of vertex id to messages which were sent
   */
  public Map<Integer, Set<MV>> getAllMessages() {
    final Map<Integer, Set<MV>> map = Maps.newHashMap();
    messageMap.entrySet().forEach(entry -> map.putAll(entry.getValue()));
    return map;
  }

}
