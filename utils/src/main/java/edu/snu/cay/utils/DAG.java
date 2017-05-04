/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.utils;

import java.util.Set;

/**
 * This interface represents Directed Acyclic Graph (DAG).
 * It is based on the one of MIST.
 * @param <V> type of the vertex
 */
public interface DAG<V> {

  /**
   * Gets root vertices for graph traversal.
   * @return set of root vertices
   */
  Set<V> getRootVertices();

  /**
   * Checks whether there is an edge from the vertices v to w.
   * @param v src vertex
   * @param w dest vertex
   * @return true if there exists an edge from v to w, otherwise false.
   */
  boolean isAdjacent(V v, V w);

  /**
   * Gets all vertices w such that there is an edge from the vertices v to w.
   * @param v src vertex
   * @return neighbor vertices of v
   * @throws java.util.NoSuchElementException if the vertex v does not exist.
   */
  Set<V> getNeighbors(V v);

  /**
   * Adds the vertex v, if it is not there.
   * @param v vertex
   * @return true if the vertex is added, false if the vertex already exists
   */
  boolean addVertex(V v);

  /**
   * Removes the vertex v, if it is there.
   * It also removes remaining edges linked with the vertex.
   * @param v vertex
   * @return true if the vertex is removed, false if the vertex does not exist
   */
  boolean removeVertex(V v);

  /**
   * Adds the edge from the vertices v to w, if it is not there.
   * @param v src vertex
   * @param w dest vertex
   * @return true if the edge is added, false if the edge already exists between v and w
   * @throws java.util.NoSuchElementException if the vertex v or w does not exist
   * @throws IllegalStateException if the added edge generates a cycle in the graph
   */
  boolean addEdge(V v, V w);

  /**
   * Removes the edge from the vertices v to w, if it is there.
   * @param v src vertex
   * @param w dest vertex
   * @return true if the edge is removed, false if the edge does not exist between v and w
   * @throws java.util.NoSuchElementException if the vertex v or w do not exist
   */
  boolean removeEdge(V v, V w);

  /**
   * Gets the in-degree of vertex v.
   * @param v vertex
   * @return in-degree of vertex v
   * @throws java.util.NoSuchElementException if the vertex v does not exist.
   */
  int getInDegree(V v);

  /**
   * Gets the current number of vertices in DAG.
   * @return current number of vertices.
   */
  int getNumVertices();

  /**
   * Gets the json string of root vertices and an adjacent list.
   * @return json string of root vertices and an adjacent list.
   */
  String toJSON();
}
