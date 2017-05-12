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
import edu.snu.cay.pregel.graph.api.Computation;
import edu.snu.cay.pregel.graph.api.Vertex;

import java.util.logging.Logger;

/**
 * Implementation of {@link Computation} to execute a pagerank algorithm.
 */
public class PagerankComputation implements Computation<Double, Double, Double> {

  private static final Logger LOG = Logger.getLogger(PagerankComputation.class.getName());
  private static final double DAMPING_FACTOR = 0.85f;
  private static final int NUM_TOTAL_SUPERSTEP = 10;

  private final Integer superstep;

  private final MessageManager<Double> messageManager;

  public PagerankComputation(final Integer superstep,
                             final MessageManager<Double> messageManager) {

    this.superstep = superstep;
    this.messageManager = messageManager;
  }

  @Override
  public void compute(final Vertex<Double> vertex, final Iterable<Double> messages) {
    if (getSuperstep() == 0) {
      vertex.setValue(1d);
      sendMessagesToAdjacents(vertex, vertex.getValue() / vertex.getNumEdges());
      return;
    }
    final double sum = Lists.newArrayList(messages).stream().mapToDouble(Double::doubleValue).sum();
    vertex.setValue((1 - DAMPING_FACTOR) + DAMPING_FACTOR * sum);
    sendMessagesToAdjacents(vertex, vertex.getValue() / vertex.getNumEdges());

    if (getSuperstep() >= NUM_TOTAL_SUPERSTEP) {
      vertex.voteToHalt();
    }
  }

  @Override
  public int getSuperstep() {
    return superstep;
  }

  @Override
  public void sendMessage(final Integer id, final Double message) {
    messageManager.writeMessage(id, message);
  }

  @Override
  public void sendMessagesToAdjacents(final Vertex<Double> vertex, final Double message) {
    vertex.getEdges().forEach(edge -> messageManager.writeMessage(edge.getTargetVertexId(), message));
  }
}
