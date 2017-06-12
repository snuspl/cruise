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
 * Default implementation of {@link Vertex}.
 *
 * @param <V> vertex id
 */
public class DefaultVertex<V> implements Vertex<V> {

  private Long id;

  private V value;

  private List<Edge> edges;

  private boolean isHalt;

  @Override
  public void initialize(final Long vertexId, final V vertexValue) {
    this.id = vertexId;
    this.value = vertexValue;
    this.edges = Lists.newArrayList();
    isHalt = false;
  }

  @Override
  public void initialize(final Long vertexId, final Iterable<Edge> adjacentEdges) {
    this.id = vertexId;
    this.edges = Lists.newArrayList();
    this.edges.addAll(Lists.newArrayList(adjacentEdges));
    isHalt = false;
  }

  @Override
  public void initialize(final Long vertexId, final V vertexValue, final Iterable<Edge> adjacentEdges) {
    this.id = vertexId;
    this.value = vertexValue;
    this.edges = Lists.newArrayList();
    this.edges.addAll(Lists.newArrayList(adjacentEdges));
    isHalt = false;
  }

  @Override
  public Long getId() {
    return id;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public void setValue(final V value) {
    this.value = value;
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
  public Iterable<Edge> getEdges() {
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
