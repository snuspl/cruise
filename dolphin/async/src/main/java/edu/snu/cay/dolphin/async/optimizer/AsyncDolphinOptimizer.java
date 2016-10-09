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
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.dolphin.async.plan.EmptyPlan;
import edu.snu.cay.dolphin.async.plan.PlanImpl;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.impl.TransferStepImpl;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import edu.snu.cay.services.ps.server.parameters.ServerNumThreads;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.ToDoubleFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Uses metrics collected from workers and servers to estimate the cost associated with epoch time.
 * It generates an optimization plan for the system that minimizes the cost.
 *
 * The cost model is based on computation cost + communication cost where:
 * computation cost = Avg. time to process a computation * No. of total training data instances / No. of workers
 * communication cost = Avg. time to process a pull request * No. of total model keys * No. of workers /
 * (No. of servers * No. of threads per server)
 */
public final class AsyncDolphinOptimizer implements Optimizer {
  private static final Logger LOG = Logger.getLogger(AsyncDolphinOptimizer.class.getName());
  private static final String NEW_WORKER_ID_PREFIX = "NewWorker-";
  private static final String NEW_SERVER_ID_PREFIX = "NewServer-";

  private final int miniBatchSize;
  private final int numServerThreads;
  private final double optBenefitThreshold;

  @Inject
  private AsyncDolphinOptimizer(@Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize,
                                @Parameter(ServerNumThreads.class) final int numServerThreads,
                                @Parameter(Parameters.OptimizationBenefitThreshold.class)
                                final double optBenefitThreshold) {
    this.miniBatchSize = miniBatchSize;
    this.optBenefitThreshold = optBenefitThreshold;
    this.numServerThreads = numServerThreads;
  }

  /**
   * Comparator to sort {@link EvaluatorSummary}s put in a priority queue, by the evaluators' throughput.
   * {@link EvaluatorSummary}s with greater throughput (i.e. quicker evaluators)
   * are put at the front (descending order).
   */
  private static final Comparator<EvaluatorSummary> THROUGHPUT_COMPARATOR =
      new Comparator<EvaluatorSummary>() {
        @Override
        public int compare(final EvaluatorSummary o1, final EvaluatorSummary o2) {
          if (o1.getThroughput() < o2.getThroughput()) {
            return 1;
          } else if (o1.getThroughput() > o2.getThroughput()) {
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
  public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators,
                       final Map<String, Double> modelParamsMap) {
    final List<EvaluatorParameters> serverParams = evalParamsMap.get(Constants.NAMESPACE_SERVER);
    final List<EvaluatorParameters> workerParams = evalParamsMap.get(Constants.NAMESPACE_WORKER);

    final int numAvailableExtraEvals = availableEvaluators - (serverParams.size() + workerParams.size());

    final Pair<List<EvaluatorSummary>, Integer> serverPair =
        sortEvaluatorsByThroughput(serverParams, availableEvaluators,
            param -> ((ServerMetrics) param.getMetrics()).getTotalPullProcessingTimeSec() /
                (double) ((ServerMetrics) param.getMetrics()).getTotalPullProcessed(),
            NEW_SERVER_ID_PREFIX);
    final List<EvaluatorSummary> serverSummaries = serverPair.getFirst();
    final int numModelBlocks = serverPair.getSecond();

    final Pair<List<EvaluatorSummary>, Integer> workerPair =
        sortEvaluatorsByThroughput(workerParams, availableEvaluators,
            param -> ((WorkerMetrics) param.getMetrics()).getTotalCompTime() /
            (double) ((WorkerMetrics) param.getMetrics()).getProcessedDataItemCount(),
            NEW_WORKER_ID_PREFIX);
    final List<EvaluatorSummary> workerSummaries = workerPair.getFirst();
    final int numDataBlocks = workerPair.getSecond();

    final int numTotalDataInstances = modelParamsMap.get(Constants.TOTAL_DATA_INSTANCES).intValue();
    final double numTotalModelKeys = modelParamsMap.get(Constants.TOTAL_PULLS_PER_MINI_BATCH);
    /*
     * 1. for each possible number of workers, check and filter:
     * a) total number of data blocks on worker side should be greater than or equal to the number of workers
     * b) total number of model blocks on server side should be greater than or equal to the number of servers
     *
     * 2. for each possible number of workers after 1, calculate cost with current metrics (avg) according to the model
     * 3. compare the total costs for each possible number of workers
     * 4. set optimalNumWorkers to be one that has the minimum total cost
     */
    final Pair<Integer, Pair<Double, Double>> optimalNumWorkersCostPair = IntStream.range(1, availableEvaluators)
        .filter(x -> x <= numDataBlocks && (availableEvaluators - x) <= numModelBlocks)
        .mapToObj(numWorkers ->
            new Pair<>(numWorkers,
                totalCost(numWorkers, numTotalDataInstances, numTotalModelKeys,
                    availableEvaluators, workerSummaries, serverSummaries)))
        .reduce(new BinaryOperator<Pair<Integer, Pair<Double, Double>>>() {
          @Override
          public Pair<Integer, Pair<Double, Double>> apply(final Pair<Integer, Pair<Double, Double>> p1,
                                                           final Pair<Integer, Pair<Double, Double>> p2) {
            final double cost1 = p1.getSecond().getFirst() + p1.getSecond().getSecond();
            final double cost2 = p2.getSecond().getFirst() + p2.getSecond().getSecond();

            return cost1 > cost2 ? p2 : p1;
          }
        })
        .get();

    final int currentNumWorkers = workerParams.size();
    final int currentNumServers = serverParams.size();

    final int optimalNumWorkers = optimalNumWorkersCostPair.getFirst();
    final int optimalNumServers = availableEvaluators - optimalNumWorkers;

    final double optimalCompCost = optimalNumWorkersCostPair.getSecond().getFirst();
    final double optimalCommCost = optimalNumWorkersCostPair.getSecond().getSecond();

    final double currentCompCost = workerParams.stream()
        .mapToDouble(param -> ((WorkerMetrics) param.getMetrics()).getTotalCompTime()).average().orElse(0D);
    final double currentCommCost = workerParams.stream()
        .mapToDouble(param -> ((WorkerMetrics) param.getMetrics()).getTotalPullTime()).average().orElse(0D);

    final String optimizationInfo = String.format("{\"numAvailEval\":%d, " +
            "\"optNumWorker\":%d, \"currNumWorker\":%d, \"optNumServer\":%d, \"currNumServer\":%d, " +
            "\"optCompCost\":%f, \"currCompCost\":%f, \"optCommCost\":%f, \"currCommCost\":%f," +
            "\"optBenefitThreshold\":%f}", availableEvaluators,
        optimalNumWorkers, currentNumWorkers, optimalNumServers, currentNumServers,
        optimalCompCost, currentCompCost, optimalCommCost, currentCommCost, optBenefitThreshold);

    LOG.log(Level.INFO, "OptimizationInfo {0} {1}", new Object[]{System.currentTimeMillis(), optimizationInfo});

    final double currentTotalCost = currentCompCost + currentCommCost;
    final double optimalTotalCost = optimalCompCost + optimalCommCost;
    // A valid reconfiguration plan is generated only when optimizer determines that a reconfiguration should occur.
    if ((currentTotalCost - optimalTotalCost) / currentTotalCost < optBenefitThreshold) {
      return new EmptyPlan();
    } else {
      final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();
      generatePlanForOptimalConfig(Constants.NAMESPACE_SERVER, serverSummaries, optimalNumServers,
          serverParams.size(), numModelBlocks, planBuilder);
      generatePlanForOptimalConfig(Constants.NAMESPACE_WORKER, workerSummaries, optimalNumWorkers,
          workerParams.size(), numDataBlocks, planBuilder);

      planBuilder.setNumAvailableExtraEvaluators(numAvailableExtraEvals > 0 ? numAvailableExtraEvals : 0);

      return planBuilder.build();
    }
  }

  /**
   * Generates an execution plan given the optimal number of evaluators for a particular namespace.
   *
   * @param namespace SERVER | WORKER
   * @param evaluatorSummaries {@link EvaluatorSummary} for the namespace
   * @param optimalEvalForNamespace optimal number of evaluators for the namespace
   * @param activeEvalForNamespace number of evaluators currently running in the namespace
   * @param totalBlocksInNamespace total number of blocks in the namespace (data for workers, model for servers)
   * @param planBuilder a builder for the optimization plan
   */
  private void generatePlanForOptimalConfig(
      final String namespace,
      final List<EvaluatorSummary> evaluatorSummaries,
      final int optimalEvalForNamespace,
      final int activeEvalForNamespace,
      final int totalBlocksInNamespace,
      final PlanImpl.Builder planBuilder) {

    // assign optimal number of blocks for each evaluator using throughput
    final double throughputSum = evaluatorSummaries.subList(0, optimalEvalForNamespace).stream()
        .mapToDouble(evaluator -> evaluator.throughput)
        .sum();
    int numAssignedBlocks = 0;

    for (int evalIndex = 0; evalIndex < optimalEvalForNamespace; ++evalIndex) {
      final EvaluatorSummary evaluator = evaluatorSummaries.get(evalIndex);

      // the last evaluator takes all remaining blocks
      if (evalIndex == optimalEvalForNamespace - 1) {
        evaluator.setNumOptimalBlocks(totalBlocksInNamespace - numAssignedBlocks);

      } else {
        final int numOptimalBlocks =
            (int) Math.round(totalBlocksInNamespace * evaluator.getThroughput() / throughputSum);
        numAssignedBlocks += numOptimalBlocks;
        evaluator.setNumOptimalBlocks(numOptimalBlocks);
      }
    }

    if (activeEvalForNamespace > optimalEvalForNamespace) {
      // delete excess evaluators and un-assign blocks so that data migration plan can be generated accordingly
      for (int evalIndex = optimalEvalForNamespace; evalIndex < activeEvalForNamespace; ++evalIndex) {
        final EvaluatorSummary evaluator = evaluatorSummaries.get(evalIndex);
        planBuilder.addEvaluatorToDelete(namespace, evaluator.getId());
        evaluator.setNumOptimalBlocks(0);
      }
    } else if (activeEvalForNamespace < optimalEvalForNamespace) {
      // add evaluators if necessary
      for (int evalIndex = activeEvalForNamespace; evalIndex < optimalEvalForNamespace; ++evalIndex) {
        final EvaluatorSummary evaluator = evaluatorSummaries.get(evalIndex);
        planBuilder.addEvaluatorToAdd(namespace, evaluator.getId());
      }
    }

    generateTransferSteps(namespace,
        evaluatorSummaries.subList(0, Math.max(optimalEvalForNamespace, activeEvalForNamespace)),
        planBuilder);
  }

  /**
   * Sorts the evaluator nodes according to the throughput comparator and generates a list of each evaluator's summary.
   * Append {@link EvaluatorSummary} for nodes that can be added for the extra room.
   *
   * @param params parameters related to an evaluator
   * @param availableEvaluators number of total evaluators in the system
   * @param unitCostFunc (e.g., server - processing time per pull / worker - computation time per data block)
   * @param newNodeIdPrefix prefix for the new nodes that can be added
   * @return {@link EvaluatorSummary} list sorted according to the throughput
   */
  private Pair<List<EvaluatorSummary>, Integer> sortEvaluatorsByThroughput(
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

    // throughput = server: processable pull per unit time | worker: processable data blocks per unit time
    final List<EvaluatorSummary> nodes = params.stream()
        .map(param -> new EvaluatorSummary(param.getId(), param.getDataInfo(), 1 / unitCostFunc.applyAsDouble(param)))
        .collect(Collectors.toList());

    // sorted in the order of high "throughput"
    Collections.sort(nodes, THROUGHPUT_COMPARATOR);

    final double unitCostAvg = unitCostSum / params.size();
    final double throughput = 1 / unitCostAvg;

    // We can add up to (availableEvaluators - runningEvaluators - 1) evaluators in each namespace,
    // and reserve at least one evaluator for the other namespace.
    for (int index = 0; index < availableEvaluators - params.size() - 1; ++index) {
      nodes.add(new EvaluatorSummary(newNodeIdPrefix + index, new DataInfoImpl(), throughput));
    }

    return new Pair<>(nodes, numBlocksTotal);
  }

  /**
   * Calculates total cost (computation cost and communication cost) of the system under optimization.
   *
   * @param numWorker given number of workers
   * @param numTotalDataInstances total number of data instances across workers
   * @param numTotalModelKeys total number of model keys across servers
   * @param availableEvaluators number of evaluators available
   * @param workers list of worker {@link EvaluatorSummary}
   * @param servers list of server {@link EvaluatorSummary}
   * @return total cost for a given number of workers using the current metrics of the system
   */
  private Pair<Double, Double> totalCost(final int numWorker,
                                         final int numTotalDataInstances,
                                         final double numTotalModelKeys,
                                         final int availableEvaluators,
                                         final List<EvaluatorSummary> workers,
                                         final List<EvaluatorSummary> servers) {
    // Calculating an estimate of the No. of mini-batches when numWorker workers are used.
    double avgNumMiniBatchesPerWorker = Math.ceil((double) numTotalDataInstances / numWorker / miniBatchSize);

    // Calculating compCost based on avg: (avgNumDataInstancesPerWorker / avgThroughput)
    final double workerThroughputSum = workers.subList(0, numWorker).stream()
        .mapToDouble(worker -> worker.throughput)
        .sum();
    final double compCost = numTotalDataInstances / workerThroughputSum;

    // Calculating commCost based on avg: (avgNumModelKeysPerServer / avgThroughput)
    final int numServer = availableEvaluators - numWorker;

    // serverThroughputSum of throughput values per server thread
    final double serverThroughputSum = servers.subList(0, numServer).stream()
        .mapToDouble(server -> server.throughput)
        .sum();
    final double commCost = numTotalModelKeys / serverThroughputSum / numServerThreads
        * numWorker * avgNumMiniBatchesPerWorker;

    final double totalCost = compCost + commCost;
    final String costInfo = String.format("{\"numServer\": %d, \"numWorker\": %d, \"totalCost\": %f, " +
        "\"compCost\": %f, \"commCost\": %f}", numServer, numWorker, totalCost, compCost, commCost);
    LOG.log(Level.INFO, "CostInfo {0} {1}", new Object[]{System.currentTimeMillis(), costInfo});
    return new Pair<>(compCost, commCost);
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

      builder.addTransferStep(namespace, new TransferStepImpl(sender.getId(),
          receiver.getId(), new DataInfoImpl(numToMove)));

      // if there are more blocks to be sent/received,
      // the sending/receiving evaluator is added back to the PQ with updated numBlocks.
      if (numToSend == numToReceive) {
        continue;
      } else if (numToMove == numToSend) {
        receiver.setNumBlocks(receiver.getNumBlocks() + numToMove);
        receiverPriorityQueue.add(receiver);
      } else { // if (numToMove == numToReceive)
        sender.setNumBlocks(sender.getNumBlocks() - numToMove);
        senderPriorityQueue.add(sender);
      }
    }
  }

  /**
   * A summary of the number of data blocks at an evaluator and the number of optimal blocks.
   */
  private static final class EvaluatorSummary {
    private final String id;
    private final DataInfo dataInfo;
    private final double throughput;
    private int numOptimalBlocks;

    private EvaluatorSummary(final String id, final DataInfo dataInfo, final double throughput) {
      this.id = id;
      this.throughput = throughput;
      this.dataInfo = dataInfo;
    }

    public String getId() {
      return id;
    }

    private void setNumOptimalBlocks(final int numOptimalBlocks) {
      this.numOptimalBlocks = numOptimalBlocks;
    }

    public int getNumBlocks() {
      return dataInfo.getNumBlocks();
    }

    public void setNumBlocks(final int numBlocks) {
      dataInfo.setNumBlocks(numBlocks);
    }

    public double getThroughput() {
      return this.throughput;
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
          .append(", throughput=")
          .append(throughput)
          .append(", numOptimalBlocks=")
          .append(numOptimalBlocks)
          .append("]");
      return sb.toString();
    }
  }
}
