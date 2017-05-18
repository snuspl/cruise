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
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Task class to run a Pregel app.
 */
@EvaluatorSide
public final class PregelWorkerTask implements Task {


  private static final Logger LOG = Logger.getLogger(PregelWorkerTask.class.getName());

  /**
   * the number of threads worker runs in each superstep.
   */
  private static final int NUM_THREADS = 3;

  /**
   * Manage message stores in this works.
   */
  private final MessageManager<Long, Double> messageManager;

  private final WorkerMsgManager workerMsgManager;

  private final TableAccessor tableAccessor;

  /**
   * The number of active vertices in this graph partitions which were allocated to this worker.
   * This value is set at the end of each superstep.
   * It is used to determine whether task finishes or not by {@link WorkerMsgManager}
   */
  private final AtomicInteger numActiveVertices = new AtomicInteger(0);

  @Inject
  private PregelWorkerTask(final MessageManager messageManager,
                           final WorkerMsgManager workerMsgManager,
                           final TableAccessor tableAccessor) {
    this.messageManager = messageManager;
    this.workerMsgManager = workerMsgManager;
    this.tableAccessor = tableAccessor;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {

    LOG.log(Level.INFO, "Pregel task start");

    final int numThreads = NUM_THREADS;

    final AtomicInteger superStepCounter = new AtomicInteger(0);
    final Table<Long, Vertex<Double>, Double> vertexTable = tableAccessor.getTable(PregelDriver.VERTEX_TABLE_ID);
    numActiveVertices.set(vertexTable.getLocalTablet().getDataMap().size());

    // run until all vertices halt
    while (true) {

      final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
      final Computation<Double, Double> computation =
          new PagerankComputation(superStepCounter.get(), messageManager.getNextMessageTable());
      final List<Future<Integer>> futureList = new ArrayList<>(numThreads);

      final Map<Long, Vertex<Double>> localVertexMap = vertexTable.getLocalTablet().getDataMap();
      final List<Vertex<Double>> localVertcesList = Lists.newArrayList(localVertexMap.values());
      final List<List<Vertex<Double>>> vertexMapPartitions = Lists.partition(localVertcesList,
          localVertexMap.size() / numThreads);

      for (int threadIdx = 0; threadIdx < numThreads; threadIdx++) {
        final Callable<Integer> computationCallable =
            new ComputationCallable<>(computation, vertexMapPartitions.get(threadIdx),
                messageManager.getCurrentMessageTable());
        futureList.add(executorService.submit(computationCallable));
      }

      numActiveVertices.set(0);
      futureList.forEach(future -> {
        try {
          numActiveVertices.getAndAdd(future.get());
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });

      LOG.log(Level.INFO, "Superstep {0} is finished", superStepCounter.get());

      if (!workerMsgManager.waitForTryNextSuperstepMsg(numActiveVertices.get())) {
        break;
      }

      messageManager.prepareForNextSuperstep();
      superStepCounter.getAndIncrement();
    }

    vertexTable.getLocalTablet().getDataMap().values().forEach(vertex -> {
      LOG.log(Level.INFO, "Vertex id : {0}, value : {1}", new Object[]{vertex.getId(), vertex.getValue()});
    });

    return null;
  }

}
