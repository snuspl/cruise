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

import edu.snu.cay.pregel.graph.api.Computation;
import edu.snu.cay.pregel.graph.api.Vertex;
import edu.snu.cay.services.et.evaluator.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Compute around the verticesPartition in the partitionStore. Every thread will has
 * its instance. It is instantiated at the start time of every superstep.
 *
 * @param <V> vertex value
 */
public class ComputationCallable<V, M> implements Callable<Integer> {

  private final Computation<V, M> computation;
  private final Iterable<Vertex<V>> verticesPartition;
  private final Table<Long, List<M>, M> currMessageTable;

  public ComputationCallable(final Computation<V, M> computation,
                             final Iterable<Vertex<V>> verticesPartition,
                             final Table<Long, List<M>, M> currMessageTable) {

    this.computation = computation;
    this.verticesPartition = verticesPartition;
    this.currMessageTable = currMessageTable;
  }

  /**
   * Compute around the verticesPartition in one superstep.
   *
   * @return the number of active verticesPartition in this partitionStore
   */
  @Override
  public Integer call() throws Exception {

    final AtomicInteger numActiveVertices = new AtomicInteger(0);

    verticesPartition.forEach(vertex -> {
      try {
        computation.compute(vertex, currMessageTable.get(vertex.getId()).get());
        currMessageTable.put(vertex.getId(), new ArrayList<>()).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      if (!vertex.isHalted()) {
        numActiveVertices.getAndIncrement();
      }
    });

    computation.sync();
    return numActiveVertices.get();
  }
}
