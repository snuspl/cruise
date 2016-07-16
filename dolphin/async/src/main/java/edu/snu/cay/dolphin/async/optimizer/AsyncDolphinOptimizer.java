/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.dolphin.async.optimizer;

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.metric.WorkerConstants;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.impl.PlanImpl;
import edu.snu.cay.services.em.plan.impl.TransferStepImpl;
import edu.snu.cay.services.ps.metric.ServerConstants;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Uses metrics collected from workers and servers to estimate the cost associated with iteration time.
 * It generates an optimization plan for the system that minimizes the cost.
 *
 * The cost model is based on computation cost + communication cost where:
 * computation cost = workers' computation time averaged
 * communication cost = servers' pull processing time averaged
 */
public final class AsyncDolphinOptimizer implements Optimizer {
  private static final String NEW_WORKER_ID_PREFIX = "NewWorker-";
  private static final String NEW_SERVER_ID_PREFIX = "NewServer-";

  private final int numMiniBatchPerItr;

  @Inject
  private AsyncDolphinOptimizer(@Parameter(Parameters.MiniBatches.class) final int numMiniBatchPerItr) {
    this.numMiniBatchPerItr = numMiniBatchPerItr;
  }

  /**
   * Comparator to sort {@link EvaluatorSummary}s put in a priority queue, by their inverse computation unit costs.
   * {@link EvaluatorSummary}s with greater inverse computation unit costs (i.e. quicker evaluators)
   * are put at the front (descending order).
   */
  private static final Comparator<EvaluatorSummary> UNIT_COST_INV_COMPARATOR =
      new Comparator<EvaluatorSummary>() {
        @Override
        public int compare(final EvaluatorSummary o1, final EvaluatorSummary o2) {
          if (o1.getUnitCostInv() < o2.getUnitCostInv()) {
            return 1;
          } else if (o1.getUnitCostInv() > o2.getUnitCostInv()) {
            return -1;
          } else {
            return 0;
          }
        }
      };

  /**
   * Comparator to sort {@link EvaluatorSummary}s put in a priority queue, by their number of blocks to move.
   * {@link EvaluatorSummary}s with more blocks to move are put at the front (descending order).
   */
  private static final Comparator<EvaluatorSummary> NUM_BLOCKS_TO_MOVE_COMPARATOR =
      new Comparator<EvaluatorSummary>() {
        @Override
        public int compare(final EvaluatorSummary o1, final EvaluatorSummary o2) {
          final int numBlocksToMove1 = Math.abs(o1.getNumBlocks() - o1.getNumOptimalBlocks());
          final int numBlocksToMove2 = Math.abs(o2.getNumBlocks() - o2.getNumOptimalBlocks());
          return numBlocksToMove2 - numBlocksToMove1;
        }
      };

  @Override
  public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators) {
    final List<EvaluatorParameters> serverParams = evalParamsMap.get(OptimizationOrchestrator.NAMESPACE_SERVER);
    final List<EvaluatorParameters> workerParams = evalParamsMap.get(OptimizationOrchestrator.NAMESPACE_WORKER);

    final Pair<List<EvaluatorSummary>, Integer> serverPair = sortNodes(serverParams, availableEvaluators,
        param -> param.getMetrics().get(ServerConstants.SERVER_PROCESSING_TIME),
        NEW_SERVER_ID_PREFIX);
    final List<EvaluatorSummary> servers = serverPair.getFirst();
    final int numModelBlocks = serverPair.getSecond();

    final Pair<List<EvaluatorSummary>, Integer> workerPair = sortNodes(workerParams, availableEvaluators,
        param -> param.getMetrics().get(WorkerConstants.WORKER_COMPUTE_TIME) / param.getDataInfo().getNumBlocks(),
        NEW_WORKER_ID_PREFIX);
    final List<EvaluatorSummary> workers = workerPair.getFirst();
    final int numDataBlocks = workerPair.getSecond();

    /*
     * 1. for each possible number of workers, check and filter:
     * a) total number of data blocks on worker side should be greater than the number of workers
     * b) total number of model blocks on server side should be greater than the number of servers
     *
     * 2. for each possible number of workers after 1, calculate cost with current metrics (avg) according to the model
     * 3. compare the total costs for each possible number of workers
     * 4. set optimalNumWorkers to be one that has the minimum total cost
     */
    final int optimalNumWorkers = IntStream.range(1, availableEvaluators)
        .filter(x -> x <= numDataBlocks && (availableEvaluators - x) <= numModelBlocks)
        .mapToObj(numWorkers ->
            new Pair<>(numWorkers,
                totalCost(numWorkers, numDataBlocks, numModelBlocks, availableEvaluators, workers, servers)))
        .reduce((p1, p2) -> p1.getSecond() > p2.getSecond() ? p2 : p1)
        .get()
        .getFirst();
    final int optimalNumServers = availableEvaluators - optimalNumWorkers;

    final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();

    // assign optimal number of blocks for each worker using unitCostInv
    final double workerUnitCostInvSum = workers.subList(0, optimalNumWorkers).stream()
        .mapToDouble(worker -> worker.unitCostInv)
        .sum();
    int numAssignedDataBlocks = 0;

    for (int workerIndex = 0; workerIndex < optimalNumWorkers; ++workerIndex) {
      final EvaluatorSummary worker = workers.get(workerIndex);
      if (workerIndex == optimalNumWorkers - 1) {
        worker.setNumOptimalBlocks(numDataBlocks - numAssignedDataBlocks);

      } else {
        final int numOptimalBlocks = (int) Math.round(numDataBlocks * worker.getUnitCostInv() / workerUnitCostInvSum);
        numAssignedDataBlocks += numOptimalBlocks;
        worker.setNumOptimalBlocks(numOptimalBlocks);
      }
    }

    // delete excess workers and un-assign blocks
    for (int workerIndex = optimalNumWorkers; workerIndex < workerParams.size(); ++workerIndex) {
      final EvaluatorSummary worker = workers.get(workerIndex);
      planBuilder.addEvaluatorToDelete(OptimizationOrchestrator.NAMESPACE_WORKER, worker.id);
      worker.setNumOptimalBlocks(0);
    }

    // assign optimal number of blocks for each server using unitCostInv
    final double serverUnitCostInvSum = servers.subList(0, optimalNumServers).stream()
        .mapToDouble(server -> server.unitCostInv)
        .sum();
    int numAssignedModelBlocks = 0;
    for (int serverIndex = 0; serverIndex < optimalNumServers; ++serverIndex) {
      final EvaluatorSummary server = servers.get(serverIndex);
      if (serverIndex == optimalNumServers - 1) {
        server.setNumOptimalBlocks(numModelBlocks - numAssignedModelBlocks);

      } else {
        final int numOptimalBlocks = (int) Math.round(numModelBlocks * server.getUnitCostInv() / serverUnitCostInvSum);
        numAssignedModelBlocks += numOptimalBlocks;
        server.setNumOptimalBlocks(numOptimalBlocks);
      }
    }

    // delete excess servers and un-assign blocks
    for (int serverIndex = optimalNumServers; serverIndex < serverParams.size(); ++serverIndex) {
      final EvaluatorSummary server = servers.get(serverIndex);
      planBuilder.addEvaluatorToDelete(OptimizationOrchestrator.NAMESPACE_SERVER, server.id);
      server.setNumOptimalBlocks(0);
    }

    // add worker evaluators if necessary
    for (int workerIndex = workerParams.size(); workerIndex < optimalNumWorkers; ++workerIndex) {
      final EvaluatorSummary worker = workers.get(workerIndex);
      planBuilder.addEvaluatorToAdd(OptimizationOrchestrator.NAMESPACE_WORKER, worker.id);
    }

    // add server evaluators if necessary
    for (int serverIndex = serverParams.size(); serverIndex < optimalNumServers; ++serverIndex) {
      final EvaluatorSummary server = servers.get(serverIndex);
      planBuilder.addEvaluatorToAdd(OptimizationOrchestrator.NAMESPACE_SERVER, server.id);
    }

    // generate a plan for moving blocks between evaluators
    generateTransferSteps(OptimizationOrchestrator.NAMESPACE_SERVER,
        servers.subList(0, Math.max(optimalNumServers, serverParams.size())),
        planBuilder);
    generateTransferSteps(OptimizationOrchestrator.NAMESPACE_WORKER,
        workers.subList(0, Math.max(optimalNumWorkers, workerParams.size())),
        planBuilder);

    return planBuilder.build();
  }

  /**
   * Sorts the evaluator nodes according to the unit cost comparator and generates a list of each evaluator's summary.
   * Append {@link EvaluatorSummary} for nodes that can be added for the extra room.
   *
   * @param params parameters related to an evaluator
   * @param availableEvaluators number of total evaluators in the system
   * @param unitCostFunc (e.g., server - processing time per pull / worker - computation time per data block)
   * @param newNodeIdPrefix prefix for the new nodes that can be added
   * @return {@link EvaluatorSummary} list sorted according to the unitCostInv
   */
  private Pair<List<EvaluatorSummary>, Integer> sortNodes(
      final List<EvaluatorParameters> params,
      final int availableEvaluators,
      final ToDoubleFunction<EvaluatorParameters> unitCostFunc,
      final String newNodeIdPrefix) {

    final int numBlocksTotal = params.stream()
        .mapToInt(param -> param.getDataInfo().getNumBlocks())
        .sum();

    final double unitCostSum = params.stream()
        .mapToDouble(unitCostFunc)
        .sum();

    // unitCostInv = server: processable pull per unit time | worker: processable data blocks per unit time
    final List<EvaluatorSummary> nodes = params.stream()
        .map(param -> new EvaluatorSummary(param.getId(), param.getDataInfo(), 1 / unitCostFunc.applyAsDouble(param)))
        .collect(Collectors.toList());

    // sorted in the order of high "throughput"
    Collections.sort(nodes, UNIT_COST_INV_COMPARATOR);

    final double unitCostAvg = unitCostSum / params.size();
    final double unitCostAvgInv = 1 / unitCostAvg;

    // We can use up to (availableEvaluators - runningEvaluators - 1) evaluators in each namespace,
    // and reserve at least one evaluator for the other namespace.
    for (int index = 0; index < availableEvaluators - params.size() - 1; ++index) {
      nodes.add(new EvaluatorSummary(newNodeIdPrefix + index, new DataInfoImpl(), unitCostAvgInv));
    }

    return new Pair<>(nodes, numBlocksTotal);
  }

  /**
   * Calculates total cost (computation cost and communication cost) of the system under optimization.
   *
   * @param numWorker current number of workers
   * @param numDataBlocks total number of data blocks across workers
   * @param numModelBlocks total number of model blocks across servers
   * @param availableEvaluators number of evaluators available
   * @param workers list of worker {@link EvaluatorSummary}
   * @param servers list of server {@link EvaluatorSummary}
   * @return total cost for a given number of workers using the current metrics of the system
   */
  private double totalCost(final int numWorker, final int numDataBlocks, final int numModelBlocks,
                                  final int availableEvaluators,
                                  final List<EvaluatorSummary> workers,
                                  final List<EvaluatorSummary> servers) {
    // Calculating compCost based on avg: (avgNumBlockPerWorker / avgThroughput)
    final double workerUnitCostInvSum = workers.subList(0, numWorker).stream()
        .mapToDouble(worker -> worker.unitCostInv)
        .sum();
    final double compCost = numDataBlocks / workerUnitCostInvSum;

    // Calculating commCost based on avg: (avgNumBlockPerServer / avgThroughput)
    final double serverUnitCostInvSum = servers.subList(0, availableEvaluators - numWorker).stream()
        .mapToDouble(server -> server.unitCostInv)
        .sum();
    final double commCost = numModelBlocks / serverUnitCostInvSum * numWorker;

    return compCost + (commCost * numMiniBatchPerItr);
  }

  /**
   * Generates the move() operation plan according to the optimal block assignments contained in evaluatorSummaries.
   *
   * @param namespace namespace for the evaluator family
   * @param evaluatorSummaries summary of the evaluators in the system under optimization
   * @param builder a builder for the optimization plan
   */
  private static void generateTransferSteps(final String namespace,
                                            final Collection<EvaluatorSummary> evaluatorSummaries,
                                            final PlanImpl.Builder builder) {
    final PriorityQueue<EvaluatorSummary> senderPriorityQueue =
        new PriorityQueue<>(evaluatorSummaries.size(), NUM_BLOCKS_TO_MOVE_COMPARATOR);
    final PriorityQueue<EvaluatorSummary> receiverPriorityQueue =
        new PriorityQueue<>(evaluatorSummaries.size(), NUM_BLOCKS_TO_MOVE_COMPARATOR);

    for (final EvaluatorSummary eval : evaluatorSummaries) {
      if (eval.getNumBlocks() > eval.getNumOptimalBlocks()) {
        senderPriorityQueue.add(eval);
      } else if (eval.getNumBlocks() < eval.getNumOptimalBlocks()) {
        receiverPriorityQueue.add(eval);
      }
    }

    // greedy search for generating transfer steps
    while (!senderPriorityQueue.isEmpty()) {
      // pick the compute task that has the biggest amount of data blocks to send to be the sender
      final EvaluatorSummary sender = senderPriorityQueue.poll();
      final EvaluatorSummary receiver = receiverPriorityQueue.poll();

      final int numToSend = sender.getNumBlocks() - sender.getNumOptimalBlocks();
      final int numToReceive = receiver.getNumOptimalBlocks() - receiver.getNumBlocks();
      final int numToMove = Math.min(numToSend, numToReceive);

      if (numToMove == numToSend) {
        receiver.setNumOptimalBlocks(receiver.getNumOptimalBlocks() - numToMove);
        receiverPriorityQueue.add(receiver);
      } else if (numToMove == numToReceive) {
        sender.setNumOptimalBlocks(sender.getNumOptimalBlocks() + numToMove);
        senderPriorityQueue.add(sender);
      }

      builder.addTransferStep(namespace, new TransferStepImpl(sender.id, receiver.id, new DataInfoImpl(numToMove)));
    }
  }

  /**
   * A summary of the number of data blocks at an evaluator and the number of optimal blocks.
   */
  private static final class EvaluatorSummary {
    private final String id;
    private final DataInfo dataInfo;
    private final double unitCostInv;
    private int numOptimalBlocks;

    private EvaluatorSummary(final String id, final DataInfo dataInfo, final double unitCostInv) {
      this.id = id;
      this.unitCostInv = unitCostInv;
      this.dataInfo = dataInfo;
    }

    private void setNumOptimalBlocks(final int numOptimalBlocks) {
      this.numOptimalBlocks = numOptimalBlocks;
    }

    public int getNumBlocks() {
      return dataInfo.getNumBlocks();
    }

    public double getUnitCostInv() {
      return this.unitCostInv;
    }

    public int getNumOptimalBlocks() {
      return this.numOptimalBlocks;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("EvaluatorSummary[id=")
          .append(id)
          .append(", numBlocks=")
          .append(getNumBlocks())
          .append(", unitCostInv=")
          .append(unitCostInv)
          .append(", numOptimalBlocks=")
          .append(numOptimalBlocks)
          .append("]");
      return sb.toString();
    }
  }
}
