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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This implements DAG with adjacent list.
 * It is based on the one of MIST.
 * @param <V> type of the vertex
 */
@NotThreadSafe
public final class DAGImpl<V> implements DAG<V> {
  private static final Logger LOG = Logger.getLogger(DAGImpl.class.getName());

  /**
   * An adjacent list.
   */
  private final Map<V, Set<V>> adjacent = new HashMap<>();

  /**
   * A map for in-degree of vertices.
   */
  private final Map<V, Integer> inDegrees = new HashMap<>();

  /**
   * A set of root vertices.
   */
  private final Set<V> rootVertices = new HashSet<>();

  /**
   * The current number of vertices.
   */
  private int numVertices = 0;

  @Override
  public Set<V> getRootVertices() {
    return Collections.unmodifiableSet(rootVertices);
  }

  @Override
  public boolean isAdjacent(final V v, final V w) {
    final Set<V> adjs = adjacent.get(v);
    return adjs != null && adjs.contains(w);
  }

  @Override
  public Set<V> getNeighbors(final V v) {
    final Set<V> adjs = adjacent.get(v);
    if (adjs == null) {
      throw new NoSuchElementException("No src vertex " + v);
    }
    return Collections.unmodifiableSet(adjs);
  }

  @Override
  public boolean addVertex(final V v) {
    if (!adjacent.containsKey(v)) {
      adjacent.put(v, new HashSet<>());
      inDegrees.put(v, 0);
      rootVertices.add(v);
      numVertices++;
      return true;
    } else {
      LOG.log(Level.WARNING, "The vertex {0} already exists", v);
      return false;
    }
  }

  @Override
  public boolean removeVertex(final V v) {
    final Set<V> neighbors = adjacent.remove(v);
    if (neighbors != null) {
      inDegrees.remove(v);
      // update inDegrees of neighbor vertices
      // and update rootVertices
      for (final V neighbor : neighbors) {
        final int inDegree = inDegrees.get(neighbor) - 1;
        inDegrees.put(neighbor, inDegree);
        if (inDegree == 0) {
          rootVertices.add(neighbor);
        }
      }
      rootVertices.remove(v);
      numVertices--;
      return true;
    } else {
      LOG.log(Level.WARNING, "The vertex {0} does exists", v);
      return false;
    }
  }

  @Override
  public boolean addEdge(final V v, final V w) {
    if (!adjacent.containsKey(v)) {
      throw new NoSuchElementException("No src vertex " + v);
    }
    if (!adjacent.containsKey(w)) {
      throw new NoSuchElementException("No dest vertex " + w);
    }

    final Set<V> adjs = adjacent.get(v);

    if (adjs.add(w)) {
      final int inDegree = inDegrees.get(w);
      if (inDegree == 0) {
        if (rootVertices.size() == 1) {
          throw new IllegalStateException("Thd edge from " + v + " to " + w + " makes a cycle in the graph");
        }
        rootVertices.remove(w);
      }
      inDegrees.put(w, inDegree + 1);
      return true;
    } else {
      LOG.log(Level.WARNING, "The edge from {0} to {1} already exists", new Object[]{v, w});
      return false;
    }
  }

  @Override
  public boolean removeEdge(final V v, final V w) {
    final Set<V> adjs = adjacent.get(v);
    if (adjs == null) {
      throw new NoSuchElementException("No src vertex " + v);
    }

    if (adjs.remove(w)) {
      final int inDegree = inDegrees.get(w);
      inDegrees.put(w, inDegree - 1);
      if (inDegree == 1) {
        rootVertices.add(w);
      }
      return true;
    } else {
      LOG.log(Level.WARNING, "The edge from {0} to {1} does not exists", new Object[]{v, w});
      return false;
    }
  }

  @Override
  public int getInDegree(final V v) {
    final Integer inDegree = inDegrees.get(v);
    if (inDegree == null) {
      throw new NoSuchElementException("No src vertex " + v);
    }
    return inDegree;
  }

  public int getNumVertices() {
    return numVertices;
  }

  @Override
  public String toString() {
    return "DAGImpl{" +
      "rootVertices=" + rootVertices +
      ", adjacentList=" + adjacent + "}";
  }

  public String toJSON() {
    final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    final Set<String> rootVerticesOfString = rootVertices.stream().map(Object::toString).collect(Collectors.toSet());
    final Map<String, Set<String>> adjacentOfString = new HashMap<>();
    adjacent.entrySet().forEach(v -> {
      final String keyString = v.getKey().toString();
      final Set<String> setString = v.getValue().stream().map(Object::toString).collect(Collectors.toSet());
      adjacentOfString.put(keyString, setString);
    });

    return "{" + "\"rootVertices\":" + gson.toJson(rootVerticesOfString) +
        ",\"adjacent\":" + gson.toJson(adjacentOfString) + "}";
  }
}
