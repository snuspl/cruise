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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * This is an abstract helper class for users to implement their computations.
 * It implements all of the methods required by the {@link Computation}
 * interface except for the {@link #compute(Vertex, Iterable)} which we leave
 * to the user to define.
 */
public abstract class AbstractComputation<V, E, M> implements Computation<V, E, M> {

  private Integer superstep;
  /**
   * All messages are passed to this table during computation in a single superstep.
   */
  private Table<Long, List<M>, M> nextMessageTable;

  /**
   * All table commands are added the list for sync the non-blocking methods.
   * At the finish of a single superstep, worker task calls {@link #flushAllMessages()} and gets all futures in it.
   * Then clear it.
   */
  private final List<Future<?>> msgFutureList = Collections.synchronizedList(Lists.newArrayList());

  protected AbstractComputation() {

  }

  @Override
  public void initialize(final Integer currentStep, final Table<Long, List<M>, M> nextTable) {
    this.superstep = currentStep;
    this.nextMessageTable = nextTable;
  }

  @Override
  public abstract void compute(Vertex<V, E> vertex, Iterable<M> messages);

  @Override
  public int getSuperstep() {
    return superstep;
  }

  @Override
  public Future<?> sendMessage(final Long id, final M message) {
    return nextMessageTable.update(id, message);
  }

  @Override
  public List<Future<?>> sendMessagesToAdjacents(final Vertex<V, E> vertex, final M message) {
    final List<Future<?>> futureList = new ArrayList<>();
    vertex.getEdges().forEach(edge -> futureList.add(nextMessageTable.update(edge.getTargetVertexId(), message)));
    return futureList;
  }

  @Override
  public void flushAllMessages() throws ExecutionException, InterruptedException {
    for (final Future<?> msgFuture : msgFutureList) {
      msgFuture.get();
    }
    msgFutureList.clear();
  }
  
  protected List<Future<?>> getMsgFutureList() {
    return msgFutureList;
  }
}
