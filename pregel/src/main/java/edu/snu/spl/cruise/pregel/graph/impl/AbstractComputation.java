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
package edu.snu.spl.cruise.pregel.graph.impl;

import edu.snu.spl.cruise.pregel.graph.api.Computation;
import edu.snu.spl.cruise.pregel.graph.api.Vertex;

/**
 * This is an abstract helper class for users to implement their computations.
 * It implements all of the methods required by the {@link Computation}
 * interface except for the {@link #compute(Vertex, Iterable)} which we leave
 * to the user to define.
 */
public abstract class AbstractComputation<V, E, M> implements Computation<V, E, M> {

  private Integer superstep;

  private MessageManager<Long, M> messageManager;

  protected AbstractComputation(final MessageManager<Long, M> messageManager) {
    this.messageManager = messageManager;
  }

  @Override
  public void initialize(final Integer currentStep) {
    this.superstep = currentStep;
  }

  @Override
  public abstract void compute(Vertex<V, E> vertex, Iterable<M> messages);

  @Override
  public int getSuperstep() {
    return superstep;
  }

  @Override
  public void sendMessage(final Long id, final M message) {
    messageManager.addMessage(id, message);
  }

  @Override
  public void sendMessagesToAdjacents(final Vertex<V, E> vertex, final M message) {
    vertex.getEdges().forEach(edge ->
        messageManager.addMessage(edge.getTargetVertexId(), message));
  }
}
