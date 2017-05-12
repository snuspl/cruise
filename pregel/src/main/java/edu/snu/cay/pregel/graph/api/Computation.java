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
package edu.snu.cay.pregel.graph.api;

/**
 * Interface for an application for computation.
 *
 * During the superstep there can be several instance of this interface,
 * each doing computation on one partition of the graph's vertices.
 *
 * Objects of this interface only live for a single superstep.
 *
 * @param <V> vertex value.
 * @param <MI> incoming message type
 * @param <MO> outgoing message type
 */
public interface Computation<V, MI, MO> {

  /**
   * Must be defined by user to do computation on a single vertex.
   *
   * @param vertex vertex
   * @param messages messages that were sent to this vertex in the previous superstep.
   */
  void compute(Vertex<V> vertex, Iterable<MI> messages);

  /**
   * Retrieves the current superstep.
   *
   * @return current superstep
   */
  int getSuperstep();

  /**
   * Send a message to a vertex id.
   *
   * @param id vertex id to send the message to
   * @param message message data to send
   */
  void sendMessage(Integer id, MO message);

  /**
   * Send a messages to all adjacent vertices.
   *
   * @param vertex vertex
   * @param message message data to send
   */
  void sendMessagesToAdjacents(Vertex<V> vertex, MO message);

}

