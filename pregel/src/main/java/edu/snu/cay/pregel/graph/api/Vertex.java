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
 * Interface for vertex which has vertex id, value and outgoing {@link Edge}s.
 * Note that it assumes that vertex id is always {@link Long} type.
 *
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
public interface Vertex<V, E> {

  /**
   * Initialize id and value. Vertex outgoing edges will be empty.
   * This method must be called after instantiation.
   *
   * @param id vertex id
   * @param value vertex value
   */
  void initialize(Long id, V value);

  /**
   * Initialize id, outgoing edges.
   * Note that all vertices values are not initialized when graph is initialized.
   * This method must be called after instantiation.
   *
   * @param id vertex id
   * @param edges outgoing edges
   */
  void initialize(Long id, Iterable<Edge<E>> edges);

  /**
   * Initialize id, value and outgoing edges.
   * This method must be called after instantiation.
   *
   * @param id vertex id
   * @param value vertex value
   * @param edges outgoing edges
   */
  void initialize(Long id, V value, Iterable<Edge<E>> edges);

  /**
   * Get the vertex id.
   *
   * @return vertex id
   */
  Long getId();

  /**
   * Get the vertex value.
   *
   * @return vertex value
   */
  V getValue();

  /**
   * Set the vertex value.
   *
   * @param value vertex value to be set
   */
  void setValue(V value);

  /**
   * After this method is called, the compute() method will no longer be called for
   * this vertex unless a message is sent to it. The application finishes only
   * when all vertices vote to halt.
   */
  void voteToHalt();

  /**
   * Get the number of outgoing edges on this vertex.
   *
   * @return the number of outgoing edges
   */
  int getNumEdges();

  /**
   * Get a read only view of the outgoing edges of this vertex.
   *
   * @return the outgoing edges
   */
  Iterable<Edge<E>> getEdges();

  /**
   * Re-activate vertex if halted.
   */
  void wakeUp();

  /**
   * Is this vertex done?
   *
   * @return true if halted, false otherwise
   */
  boolean isHalted();
}
