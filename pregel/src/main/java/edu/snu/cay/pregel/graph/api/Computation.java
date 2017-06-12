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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Interface for an application for computation for a superstep.
 * @param <V> vertex value.
 */
public interface Computation<V, M> {

  /**
   * Must be defined by user to do computation on a single vertex.
   *
   * @param vertex vertex
   * @param messages messages that were sent to this vertex in the previous superstep.
   */
  void compute(Vertex<V> vertex, Iterable<M> messages);

  /**
   * Retrieves the current superstep.
   *
   * @return current superstep
   */
  int getSuperstep();

  /**
   * Send a message to a vertex with id {@code id}.
   *
   * @param id vertex id to send the message to
   * @param message message to send
   */
  Future<?> sendMessage(Long id, M message);

  /**
   * Send a messages to all adjacent vertices of {@code vertex}.
   *
   * @param vertex vertex
   * @param message message to send
   */
  List<Future<?>> sendMessagesToAdjacents(Vertex<V> vertex, M message);

  /**
   * Flushes out all messages sent by {@link #sendMessage} and {@link #sendMessagesToAdjacents}.
   * It returns after receiving ack messages.
   */
  void flushAllMessages() throws ExecutionException, InterruptedException;
}

