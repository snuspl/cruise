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
package edu.snu.spl.cruise.pregel.graphapps.pagerank;

import edu.snu.spl.cruise.pregel.graph.api.Computation;
import edu.snu.spl.cruise.pregel.graph.api.Vertex;
import edu.snu.spl.cruise.pregel.graph.impl.AbstractComputation;
import edu.snu.spl.cruise.pregel.graph.impl.MessageManager;

import javax.inject.Inject;

/**
 * Implementation of {@link Computation} to execute a pagerank algorithm.
 */
public final class PagerankComputation extends AbstractComputation<Double, Void, Double> {
  /**
   * Damping factor of the pagerank algorithm.
   */
  private static final double DAMPING_FACTOR = 0.85f;

  /**
   * If current superstep is same this value, all vertices vote to halt.
   */
  private static final int NUM_TOTAL_SUPERSTEP = 10;

  @Inject
  private PagerankComputation(final MessageManager<Long, Double> messageManager) {
    super(messageManager);
  }

  @Override
  public void compute(final Vertex<Double, Void> vertex, final Iterable<Double> messages) {

    if (getSuperstep() == 0) {

      // At the first superstep, each vertices doesn't get the incoming messages and
      // update the value of vertex. Because the incoming messages in the first superstep are none.
      // Instead, the value of all vertices is initialized to 1.
      vertex.setValue(1d);
    } else {

      // If incoming messages are not null, reduce it before update the value of this vertex.
      double sum = 0.0;
      for (final Double message : messages) {
        sum += message;
      }

      vertex.setValue((1 - DAMPING_FACTOR) + DAMPING_FACTOR * sum);
    }

    if (getSuperstep() < NUM_TOTAL_SUPERSTEP) {
      sendMessagesToAdjacents(vertex, vertex.getValue() / vertex.getNumEdges());
    } else {
      vertex.voteToHalt();
    }
  }
}
