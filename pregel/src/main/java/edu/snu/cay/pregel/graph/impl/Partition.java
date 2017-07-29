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

import edu.snu.cay.pregel.graph.api.Vertex;

import java.util.List;

/**
 * A partition class that represent a collection of vertices.
 * One partition is assigned to a worker thread.
 * @param <V> a value value type
 * @param <E> an edge value type
 */
public class Partition<V, E> {
  private final List<Vertex<V, E>> vertices;

  public Partition(final List<Vertex<V, E>> vertices) {
    this.vertices = vertices;
  }

  public List<Vertex<V, E>> getVertices() {
    return vertices;
  }
}
