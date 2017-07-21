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

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by cmslab on 7/20/17.
 */
public class ShortestPathComputation implements Computation<Long, List<Long>, Long> {

  @Inject
  private ShortestPathComputation() {

  }

  @Override
  public void initialize(Integer superstep, Table<Long, List<Long>, Long> nextMessageTable) {

  }

  @Override
  public void compute(Vertex<Long> vertex, Iterable<Long> messages) {

  }

  @Override
  public int getSuperstep() {
    return 0;
  }

  @Override
  public Future<?> sendMessage(Long id, Long message) {
    return null;
  }

  @Override
  public List<Future<?>> sendMessagesToAdjacents(Vertex<Long> vertex, Long message) {
    return null;
  }

  @Override
  public void flushAllMessages() throws ExecutionException, InterruptedException {

  }
}
