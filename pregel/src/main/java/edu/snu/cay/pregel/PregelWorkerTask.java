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
package edu.snu.cay.pregel;

import com.google.common.collect.Lists;
import edu.snu.cay.pregel.graph.api.Computation;
import edu.snu.cay.pregel.graph.api.Vertex;
import edu.snu.cay.pregel.graph.impl.*;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Task class to run a Pregel app.
 */
@EvaluatorSide
public final class PregelWorkerTask<V, E, M> implements Task {
  private static final Logger LOG = Logger.getLogger(PregelWorkerTask.class.getName());

  /**
   * the number of worker threads for computation.
   */
  private static final int NUM_THREADS = 3;

  /**
   * Manage message stores in this works.
   */
  private final MessageManager messageManager;

  private final WorkerMsgManager workerMsgManager;

  private final TableAccessor tableAccessor;

  private final Computation<V, E, M> computation;

  @Inject
  private PregelWorkerTask(final MessageManager messageManager,
                           final WorkerMsgManager workerMsgManager,
                           final Computation computation,
                           final TableAccessor tableAccessor) {
    this.messageManager = messageManager;
    this.workerMsgManager = workerMsgManager;
    this.computation = computation;
    this.tableAccessor = tableAccessor;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {

    LOG.log(Level.INFO, "Pregel task starts.");

    final int numThreads = NUM_THREADS;
    final ExecutorService executorService = CatchableExecutors.newFixedThreadPool(numThreads);

    int superStepCount = 0;
    final Table<Long, Vertex<V, E>, ?> vertexTable = tableAccessor.getTable(PregelDriver.VERTEX_TABLE_ID);

    // run supersteps until all vertices halt
    // each loop is a superstep
    while (true) {
      computation.initialize(superStepCount, messageManager.getNextMessageTable());
      final List<Future<Integer>> futureList = new ArrayList<>(numThreads);

      // partition local graph-dataset as the number of threads
      final Map<Long, Vertex<V, E>> vertexMap = vertexTable.getLocalTablet().getDataMap();
      final List<Vertex<V, E>> vertexList = Lists.newArrayList(vertexMap.values());
      final int numVertices = vertexList.size();
      final int sizeByPartition = numVertices / numThreads;
      final List<List<Vertex<V, E>>> vertexPartitions = new ArrayList<>(numThreads);

      int vertexIdx;
      for (int threadIdx = 0; threadIdx < numThreads; threadIdx++) {
        vertexPartitions.add(new ArrayList<>());
        for (vertexIdx = threadIdx * sizeByPartition; vertexIdx < (threadIdx + 1) * sizeByPartition; vertexIdx++) {
          vertexPartitions.get(threadIdx).add(vertexList.get(vertexIdx));
        }
        if (threadIdx == numThreads - 1) {
          while (vertexIdx < numVertices) {
            vertexPartitions.get(threadIdx).add(vertexList.get(vertexIdx));
            vertexIdx++;
          }
        }
      }

      // compute each partition with a thread pool
      for (int threadIdx = 0; threadIdx < numThreads; threadIdx++) {
        final List<Vertex<V, E>> partition = vertexPartitions.get(threadIdx);
        final Callable<Integer> computationCallable =
            new ComputationCallable<V, E, M>(computation, partition, messageManager.getCurrentMessageTable());
        futureList.add(executorService.submit(computationCallable));
      }

      // aggregate the number of active vertices from the processed partitions
      int numActiveVertices = 0;
      for (final Future<Integer> computeFuture : futureList) {
        final int numActiveVerticesInPartition = computeFuture.get();
        numActiveVertices += numActiveVerticesInPartition;
      }

      // before finishing superstep, confirm that all outgoing messages are completely sent out
      computation.flushAllMessages();

      LOG.log(Level.INFO, "Superstep {0} is finished", superStepCount);

      // master will decide whether to continue or not
      final int incomingMsgSize = messageManager.getNextMessageTable().getLocalTablet().getNumDataItems();
      final boolean continueSuperstep = workerMsgManager.waitForTryNextSuperstepMsg(numActiveVertices, incomingMsgSize);

      if (!continueSuperstep) {
        break;
      }

      // prepare next superstep
      messageManager.prepareForNextSuperstep();
      superStepCount++;
    }

    LOG.log(Level.INFO, "Pregel job has been finished after {0} supersteps.", superStepCount);
    vertexTable.getLocalTablet().getDataMap().values().forEach(vertex ->
        LOG.log(Level.INFO, "Vertex id : {0}, value : {1}", new Object[]{vertex.getId(), vertex.getValue()}));

    return null;
  }
}
