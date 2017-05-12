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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Compute around the vertices in the partition. Every thread will has
 * its instance. It is instantiated at the start time of every superstep.
 *
 * @param <V> vertex value
 * @param <MI> incoming message value
 * @param <MO> outgoing message value
 */
public class ComputationCallable<V, MI, MO> implements Callable {

  private final Computation<V, MI, MO> computation;
  private final Partition<V> partition;
  private final MessageStore<MI> currMessageStore;

  public ComputationCallable(final Computation<V, MI, MO> computation,
                             final Partition<V> partition,
                             final MessageStore<MI> currMessageStore) {

    this.computation = computation;
    this.partition = partition;
    this.currMessageStore = currMessageStore;
  }

  /**
   * Compute around the vertices in one superstep.
   *
   * @return the number of active vertices in this partition
   */
  @Override
  public Object call() throws Exception {

    partition.iterator().forEachRemaining(vertex -> {
      computation.compute(vertex, currMessageStore.getVertexMessages(vertex.getId()));
    });

    final AtomicInteger numActiveVertices = new AtomicInteger(0);
    partition.iterator().forEachRemaining(vertex -> {
      if (!vertex.isHalted()) {
        numActiveVertices.getAndIncrement();
      }
    });
    return numActiveVertices.get();
  }
}
