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

import com.google.common.collect.Lists;
import edu.snu.cay.pregel.graph.api.Edge;
import edu.snu.cay.pregel.graph.api.Vertex;

import java.util.List;

/**
 * The implementation of none-value {@link Vertex}.
 * It only has vertex id, edges
 *
 * @param <E> edge value
 */
public class NoneValueVertex<E> implements Vertex<Void, E> {

  private Long id;

  private List<Edge<E>> edges;

  private boolean isHalt;

  @Override
  public void initialize(final Long vertexId, final Void vertexValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void initialize(final Long vertexId, final Iterable<Edge<E>> adjacentEdges) {
    this.id = vertexId;
    this.edges = Lists.newArrayList();
    this.edges.addAll(Lists.newArrayList(adjacentEdges));
    isHalt = false;
  }

  @Override
  public void initialize(final Long vertexId, final Void vertexValue, final Iterable<Edge<E>> adjacentEdges) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Long getId() {
    return id;
  }

  @Override
  public Void getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValue(final Void value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void voteToHalt() {
    isHalt = true;
  }

  @Override
  public int getNumEdges() {
    return edges.size();
  }

  @Override
  public Iterable<Edge<E>> getEdges() {
    return edges;
  }

  @Override
  public void wakeUp() {
    isHalt = false;
  }

  @Override
  public boolean isHalted() {
    return isHalt;
  }
}
