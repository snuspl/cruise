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
import edu.snu.cay.services.et.evaluator.api.Table;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Compute around the verticesPartition in the partitionStore. Every thread will has
 * its instance. It is instantiated at the start time of every superstep.
 *
 * @param <V> vertex value
 * @param <E> edge value
 * @param <M> message value
 */
public class ComputationCallable<V, E, M> implements Callable<Integer> {

  private static final Logger LOG = Logger.getLogger(ComputationCallable.class.getName());
  private final Computation<V, E, M> computation;
  private final Partition<V, E> verticesPartition;
  private final Table<Long, List<M>, M> currMessageTable;

  public ComputationCallable(final Computation<V, E, M> computation,
                             final Partition<V, E> vertexPartition,
                             final Table<Long, List<M>, M> currMessageTable) {

    this.computation = computation;
    this.verticesPartition = vertexPartition;
    this.currMessageTable = currMessageTable;
  }

  /**
   * Compute around the verticesPartition in one superstep.
   *
   * @return the number of active vertices in this partition
   */
  @Override
  public Integer call() throws Exception {

    final AtomicInteger numActiveVertices = new AtomicInteger(0);

    verticesPartition.getVertices().forEach(vertex -> {
      try {

        List<M> msgsForVertex = currMessageTable.remove(vertex.getId()).get();
        if (msgsForVertex == null) {
          msgsForVertex = Collections.emptyList();
        }

        // wake up vertex, when there're incoming messages
        if (vertex.isHalted() && !msgsForVertex.isEmpty()) {
          vertex.wakeUp();
        }

        if (!vertex.isHalted()) {
          computation.compute(vertex, msgsForVertex);
        }

      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      if (!vertex.isHalted()) {
        numActiveVertices.getAndIncrement();
      }
    });

    return numActiveVertices.get();
  }
}
