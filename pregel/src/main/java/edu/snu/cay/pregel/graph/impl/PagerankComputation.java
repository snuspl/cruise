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
import edu.snu.cay.services.et.evaluator.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * Implementation of {@link Computation} to execute a pagerank algorithm.
 */
public class PagerankComputation implements Computation<Double, Double> {

  private static final Logger LOG = Logger.getLogger(PagerankComputation.class.getName());
  private static final double DAMPING_FACTOR = 0.85f;
  private static final int NUM_TOTAL_SUPERSTEP = 10;

  private final Integer superstep;

  private final Table<Long, List<Double>, Double> messageTable;

  private final List<Future<?>> msgFutureList = Lists.newArrayList();

  public PagerankComputation(final Integer superstep,
                             final Table<Long, List<Double>, Double> messageTable) {

    this.superstep = superstep;
    this.messageTable = messageTable;
  }

  @Override
  public void compute(final Vertex<Double> vertex, final Iterable<Double> messages) {

    if (getSuperstep() == 0) {

      // At the first superstep, each vertices doesn't get the incoming messages and
      // update the value of vertex. Because the incoming messages in the first superstep are none.
      // Instead, the value of all vertices is initialized to 1.
      vertex.setValue(1d);
    } else {
      final double sum = Lists.newArrayList(messages).stream().mapToDouble(Double::doubleValue).sum();
      vertex.setValue((1 - DAMPING_FACTOR) + DAMPING_FACTOR * sum);
    }
    msgFutureList.addAll(sendMessagesToAdjacents(vertex, vertex.getValue() / vertex.getNumEdges()));

    if (getSuperstep() >= NUM_TOTAL_SUPERSTEP) {
      vertex.voteToHalt();
    }
  }

  @Override
  public int getSuperstep() {
    return superstep;
  }

  @Override
  public Future<?> sendMessage(final Long id, final Double message) {
    return messageTable.update(id, message);
  }

  @Override
  public List<Future<?>> sendMessagesToAdjacents(final Vertex<Double> vertex, final Double message) {
    final List<Future<?>> futureList = new ArrayList<>();
    vertex.getEdges().forEach(edge -> futureList.add(messageTable.update(edge.getTargetVertexId(), message)));
    return futureList;
  }

  @Override
  public void sync() {
    msgFutureList.forEach(future -> {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    msgFutureList.clear();
  }
}
