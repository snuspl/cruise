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
 * Created by cmslab on 7/20/17.
 */
public abstract class AbstractComputation<V, M> implements Computation<V, M> {

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
  public void initialize(Integer currentStep, Table<Long, List<M>, M> nextTable) {
    this.superstep = currentStep;
    this.nextMessageTable = nextTable;
  }

  @Override
  public abstract void compute(Vertex<V> vertex, Iterable<M> messages);

  @Override
  public int getSuperstep() {
    return superstep;
  }

  @Override
  public Future<?> sendMessage(Long id, M message) {
    return nextMessageTable.update(id, message);
  }

  @Override
  public List<Future<?>> sendMessagesToAdjacents(Vertex<V> vertex, M message) {
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
