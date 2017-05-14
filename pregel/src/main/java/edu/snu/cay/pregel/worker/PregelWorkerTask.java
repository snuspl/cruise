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
package edu.snu.cay.pregel.worker;

import edu.snu.cay.pregel.graph.api.Computation;
import edu.snu.cay.pregel.graph.impl.*;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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


  static final List<String> INPUT = Arrays.asList(
      "1 2 3 4",
      "2 3",
      "3 1 2 4",
      "4 2");

  /**
   * A state machine representing the state of task.
   * It is initialized by {@link #initStateMachine()}.
   */
  private final StateMachine stateMachine;

  /**
   * Graph partitioner for this worker.
   */
  private final GraphPartitioner graphPartitioner;

  /**
   * Manage message stores in this works.
   */
  private final MessageManager<Double> messageManager;

  /**
   * The number of active vertices in this worker.
   * This value is set at each end of one superstep.
   * It is used to determine whether task finishes or not by {@link #isAllVerticesHalt()}
   */
  private final AtomicInteger numActiveVertices = new AtomicInteger(INPUT.size());

  @Inject
  private PregelWorkerTask(final GraphPartitioner graphPartitioner,
                           final MessageManager messageManager) {
    this.stateMachine = initStateMachine();
    this.graphPartitioner = graphPartitioner;
    this.messageManager = messageManager;
  }

  private enum State {
    INIT,
    START,
    RUN,
    RUN_FINISHING,
    COMPLETE
  }

  private static StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(State.INIT, "Workers initialize graph partitions for computation.")
        .addState(State.START, "Workers prepare for next superstep.")
        .addState(State.RUN, "Computation is running during current superstep.")
        .addState(State.RUN_FINISHING, "Computation in current superstep is finished, from now wait for other workers.")
        .addState(State.COMPLETE, "The task execution is finished. Time to clean up the task.")
        .addTransition(State.INIT, State.START, "Finish initializing the graph partitions.")
        .addTransition(State.START, State.RUN, "Finish preparing for the next superstep.")
        .addTransition(State.RUN, State.RUN_FINISHING, "Finish computation in current superstep")
        .addTransition(State.RUN_FINISHING, State.START, "All vertices don't halt")
        .addTransition(State.RUN_FINISHING, State.COMPLETE, "All vertices halt")
        .setInitialState(State.INIT)
        .build();
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {

    LOG.log(Level.INFO, "Pregel task start");

    final PartitionStore<Double> partitionStore = new PartitionStore<>(graphPartitioner, INPUT);
    final int numThreads = partitionStore.getNumPartitions();

    final AtomicInteger superStepCounter = new AtomicInteger(0);

    // run until all vertices halt
    while (true) {
      // INIT -> START or RUN_FINISHING -> START
      stateMachine.setState(State.START);
      messageManager.prepareForNextSuperstep();

      // START -> RUN
      stateMachine.setState(State.RUN);
      final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
      final Computation<Double, Double, Double> computation =
          new PagerankComputation(superStepCounter.get(), messageManager.getNextMessageStore());
      final List<Future<Integer>> futureList = new ArrayList<>(numThreads);

      for (int threadIdx = 0; threadIdx < numThreads; threadIdx++) {
        final Callable<Integer> computationCallable =
            new ComputationCallable<>(computation, partitionStore.getPartition(threadIdx),
                messageManager.getCurrentMessageStore());
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

      // RUN -> RUN_FINISHING
      stateMachine.setState(State.RUN_FINISHING);

      LOG.log(Level.INFO, "Superstep {0} is finished", superStepCounter.get());

      if (isAllVerticesHalt()) {
        break;
      }

      messageManager.prepareForNextSuperstep();
      superStepCounter.getAndIncrement();
    }

    // RUN_FINISHING -> COMPLETE
    stateMachine.setState(State.COMPLETE);

    for (int partitionIdx = 0; partitionIdx < partitionStore.getNumPartitions(); partitionIdx++) {
      partitionStore.getPartition(partitionIdx).forEach(vertex ->
          LOG.log(Level.INFO, "Vertex id : {0}, rank : {1}", new Object[]{vertex.getId(), vertex.getValue()})
      );
    }
    return null;
  }

  /**
   * Judge whether all vertices halt or not.
   *
   * @return if halt true, otherwise false
   */
  private boolean isAllVerticesHalt() {
    return numActiveVertices.get() == 0;
  }
}
