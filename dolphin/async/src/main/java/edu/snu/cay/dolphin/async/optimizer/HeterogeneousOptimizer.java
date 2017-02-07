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
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.function.ToDoubleFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This optimizer consider hardware capability of each resource, and distribute more workload to faster one.
 * This optimizer does not consider role change of existing container,
 * i.e. changing certain type of resource to act in a different role.
 */
public final class HeterogeneousOptimizer implements Optimizer {
  private static final Logger LOG = Logger.getLogger(AsyncDolphinOptimizer.class.getName());
  private static final String NEW_WORKER_ID_PREFIX = "NewWorker-";
  private static final String NEW_SERVER_ID_PREFIX = "NewServer-";

  private final int miniBatchSize;
  private final double defaultNetworkBandwidth;
  private final double optBenefitThreshold;

  private final Map<String, Double> hostnameToBandwidth;

  @Inject
  private HeterogeneousOptimizer(@Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize,
                                 @Parameter(Parameters.NetworkBandwidth.class) final double defaultNetworkBandwidth,
                                 @Parameter(Parameters.OptimizationBenefitThreshold.class)
                                 final double optBenefitThreshold) {
    this.miniBatchSize = miniBatchSize;
    // convert bits per second to bytes per second
    this.defaultNetworkBandwidth = defaultNetworkBandwidth / 8D;
    this.optBenefitThreshold = optBenefitThreshold;
    this.hostnameToBandwidth = new HashMap<>();
  }

  /**
   * Comparator to sort {@link EvaluatorSummary}s put in a priority queue, by the evaluators' throughput.
   * {@link EvaluatorSummary}s with greater throughput (i.e. quicker evaluators)
   * are put at the front (descending order).
   */
  private static final Comparator<EvaluatorSummary> THROUGHPUT_COMPARATOR =
      (o1, o2) -> {
        if (o1.getThroughput() < o2.getThroughput()) {
          return 1;
        } else if (o1.getThroughput() > o2.getThroughput()) {
          return -1;
        } else {
          return 0;
        }
      };

  /**
   * Comparator to sort {@link EvaluatorSummary}s put in a priority queue, by their number of blocks to move.
   * {@link EvaluatorSummary}s with more blocks to move are put at the front (descending order).
   */
  private static final Comparator<EvaluatorSummary> NUM_BLOCKS_TO_MOVE_COMPARATOR =
      (o1, o2) -> {
        final int numBlocksToMove1 = Math.abs(o1.getNumBlocks() - o1.getNumOptimalBlocks());
        final int numBlocksToMove2 = Math.abs(o2.getNumBlocks() - o2.getNumOptimalBlocks());
        return numBlocksToMove2 - numBlocksToMove1;
      };

  private final class OptimalResult {
    private double optimalCost;
    private int optimalNumWorkers;

    private OptimalResult() {
      this.optimalCost = Double.MAX_VALUE;
      this.optimalNumWorkers = 0;
    }

    private void replaceIfBetter(final double cost, final int numWorkers) {
      if (optimalCost > cost) {
        optimalCost = cost;
        optimalNumWorkers = numWorkers;
      }
    }
  }

  private OptimalResult chooseOptimal(final Map<Integer, Double> numWorkersCostMap) {
    final OptimalResult optimalResult = new OptimalResult();

    numWorkersCostMap.forEach((numWorkers, cost) -> optimalResult.replaceIfBetter(cost, numWorkers));
    return optimalResult;
  }

  @Override
  public Plan optimize(final Map<String, List<EvaluatorParameters>> evalParamsMap, final int availableEvaluators,
                       final Map<String, Double> modelParamsMap) {
    final List<EvaluatorParameters> serverParams = evalParamsMap.get(Constants.NAMESPACE_SERVER);
    final List<EvaluatorParameters> workerParams = evalParamsMap.get(Constants.NAMESPACE_WORKER);

    final int currUsedEvals = serverParams.size() + workerParams.size();
    final int numAvailableExtraEvals = availableEvaluators - currUsedEvals;

    final List<EvaluatorSummary> serverSummaries =
        sortEvaluatorsByThroughput(serverParams, availableEvaluators,
            param -> 1D / hostnameToBandwidth
                .getOrDefault(((ServerMetrics) param.getMetrics()).getHostname(), defaultNetworkBandwidth),
            param -> hostnameToBandwidth
                .getOrDefault(((ServerMetrics) param.getMetrics()).getHostname(), defaultNetworkBandwidth),
            NEW_SERVER_ID_PREFIX);

    final List<EvaluatorSummary> workerSummaries =
        sortEvaluatorsByThroughput(workerParams, availableEvaluators,
            param -> ((WorkerMetrics) param.getMetrics()).getTotalCompTime() /
                (double) ((WorkerMetrics) param.getMetrics()).getProcessedDataItemCount(),
            param -> hostnameToBandwidth
                .getOrDefault(((WorkerMetrics) param.getMetrics()).getHostname(), defaultNetworkBandwidth),
            NEW_WORKER_ID_PREFIX);

    final double currEstmCost;
    final double optimalCost;
    final int optimalNumServers;
    final int optimalNumWorkers;
    final int numEvalsToUse;
    final Map<Integer, Double> costMap;

    // 1. When there are extra resources to use
    if (numAvailableExtraEvals > 0) {
      // build two models with availableEvaluators and currUsedEvals
      final Map<Integer, Double> costMapWithExtraEvals = calculateCost(serverSummaries,
          workerSummaries, availableEvaluators, modelParamsMap);
      final OptimalResult optimalResultWithExtraEvals = chooseOptimal(costMapWithExtraEvals);

      final Map<Integer, Double> costMapWithCurrEvals = calculateCost(serverSummaries,
          workerSummaries, currUsedEvals, modelParamsMap);
      final OptimalResult optimalResultWithCurrEvals = chooseOptimal(costMapWithCurrEvals);

      currEstmCost = costMapWithCurrEvals.get(workerParams.size());

      // choose a configuration with extra evals only when its cost is smaller than the optimal one with current evals
      if (optimalResultWithExtraEvals.optimalCost < optimalResultWithCurrEvals.optimalCost) {
        optimalCost = optimalResultWithExtraEvals.optimalCost;
        optimalNumWorkers = optimalResultWithExtraEvals.optimalNumWorkers;
        optimalNumServers = availableEvaluators - optimalResultWithExtraEvals.optimalNumWorkers;
        costMap = costMapWithExtraEvals;
        numEvalsToUse = availableEvaluators;
      } else {
        optimalCost = optimalResultWithCurrEvals.optimalCost;
        optimalNumWorkers = optimalResultWithCurrEvals.optimalNumWorkers;
        optimalNumServers = currUsedEvals - optimalResultWithCurrEvals.optimalNumWorkers;
        costMap = costMapWithCurrEvals;
        numEvalsToUse = currUsedEvals;
      }

    } else {
      // build one model with availableEvaluators
      costMap = calculateCost(serverSummaries,
          workerSummaries, availableEvaluators, modelParamsMap);
      final OptimalResult optimalResult = chooseOptimal(costMap);

      // 2. When a portion of currently used resources should be released
      if (numAvailableExtraEvals < 0) {
        // must trigger optimization
        currEstmCost = Double.MAX_VALUE;
      } else {
        currEstmCost = costMap.get(workerParams.size());
      }
      optimalCost = optimalResult.optimalCost;
      optimalNumWorkers = optimalResult.optimalNumWorkers;
      optimalNumServers = availableEvaluators - optimalResult.optimalNumWorkers;
      numEvalsToUse = availableEvaluators;
    }

    printInfo(evalParamsMap, modelParamsMap, costMap, optimalNumWorkers, numEvalsToUse);

    // A valid reconfiguration plan is generated only when benefit is above the threshold
    return (currEstmCost - optimalCost) / currEstmCost < optBenefitThreshold ?
        new EmptyPlan() : generateOptimalPlan(serverSummaries, workerSummaries, optimalNumServers, optimalNumWorkers,
          serverParams.size(), workerParams.size(), numAvailableExtraEvals, modelParamsMap);
  }

  private void printInfo(final Map<String, List<EvaluatorParameters>> evalParamsMap,
                         final Map<String, Double> modelParamsMap,
                         final Map<Integer, Double> numWorkersCostMap,
                         final int optimalNumWorkers,
                         final int numEvalsToUse) {
    final List<EvaluatorParameters> serverParams = evalParamsMap.get(Constants.NAMESPACE_SERVER);
    final List<EvaluatorParameters> workerParams = evalParamsMap.get(Constants.NAMESPACE_WORKER);

    final StringBuilder sb = new StringBuilder();
    sb.append("[");

    numWorkersCostMap.forEach((numWorkers, cost) -> {
      sb.append(String.format("{\"numServer\": %d, \"numWorker\": %d, \"totalCost\": %f, ",
          numEvalsToUse - numWorkers, numWorkers, cost));
    });

    sb.delete(sb.length() - 2, sb.length()); // Remove trailing ', '
    sb.append("]");

    LOG.log(Level.INFO, "CostInfo {0}", sb.toString());
    final int currentNumWorkers = workerParams.size();
    final int currentNumServers = serverParams.size();

    final double optimalCost = numWorkersCostMap.get(optimalNumWorkers);

    final int numTotalDataInstances = modelParamsMap.get(Constants.TOTAL_DATA_INSTANCES).intValue();
    final double avgNumMiniBatchesPerWorker =
        Math.ceil((double) numTotalDataInstances / currentNumWorkers / miniBatchSize);

    // we must apply the costs in metrics by avgNumMiniBatchesPerWorker since these are mini-batch metrics
    final double currMeasuredCompCost = avgNumMiniBatchesPerWorker * (workerParams.stream()
        .mapToDouble(param -> ((WorkerMetrics) param.getMetrics()).getTotalCompTime()).average().orElse(0D));
    final double currMeasuredCommCost = avgNumMiniBatchesPerWorker * (workerParams.stream()
        .mapToDouble(param -> ((WorkerMetrics) param.getMetrics()).getTotalPullTime()).average().orElse(0D));
    final double currMeasuredCost = currMeasuredCompCost + currMeasuredCommCost;

    final double currEstimatedCost = numWorkersCostMap.get(currentNumWorkers);

    final String optimizationInfo = String.format("{\"numAvailEval\":%d, " +
            "\"optNumWorker\":%d, \"currNumWorker\":%d, \"optNumServer\":%d, \"currNumServer\":%d, " +
            "\"optCost\":%f, \"currEstimatedCost\":%f, \"currMeasuredCost\":%f, " +
            "\"optBenefitThreshold\":%f}", numEvalsToUse,
        optimalNumWorkers, currentNumWorkers, numEvalsToUse - optimalNumWorkers, currentNumServers,
        optimalCost, currEstimatedCost, currMeasuredCost,
        optBenefitThreshold);

    LOG.log(Level.INFO, "OptimizationInfo {0} {1}", new Object[]{System.currentTimeMillis(), optimizationInfo});
  }

  private Map<Integer, Double> calculateCost(final List<EvaluatorSummary> serverSummaries,
                                                           final List<EvaluatorSummary> workerSummaries,
                                                           final int numEvalsToUse,
                                                           final Map<String, Double> modelParamsMap) {
    final int numModelBlocks = serverSummaries.stream()
        .mapToInt(EvaluatorSummary::getNumBlocks)
        .sum();
    final int numDataBlocks = serverSummaries.stream()
        .mapToInt(EvaluatorSummary::getNumBlocks)
        .sum();

    final int numTotalDataInstances = modelParamsMap.get(Constants.TOTAL_DATA_INSTANCES).intValue();
    final double avgPullSize = modelParamsMap.get(Constants.AVG_PULL_SIZE_PER_MINI_BATCH);

    /*
     * 1. for each possible number of workers, check and filter:
     * a) total number of data blocks on worker side should be greater than or equal to the number of workers
     * b) total number of model blocks on server side should be greater than or equal to the number of servers
     *
     * 2. for each possible number of workers after 1, calculate cost with current metrics (avg) according to the model
     * 3. compare the total costs for each possible number of workers
     * 4. set optimalNumWorkers to be one that has the minimum total cost
     */
    final Map<Integer, Double> numWorkersCostMap = new HashMap<>();

    IntStream.range(1, numEvalsToUse)
        .filter(numWorkers -> numWorkers <= numDataBlocks && (numEvalsToUse - numWorkers) <= numModelBlocks)
        .forEach(numWorkers -> {
          final double totalCost = totalCost(numWorkers, numTotalDataInstances, avgPullSize,
              numEvalsToUse, workerSummaries, serverSummaries);

          numWorkersCostMap.put(numWorkers, totalCost);
        });

    return numWorkersCostMap;
  }

  private Plan generateOptimalPlan(final List<EvaluatorSummary> serverSummaries,
                                   final List<EvaluatorSummary> workerSummaries,
                                   final int optimalNumServers, final int optimalNumWorkers,
                                   final int currNumServers, final int currNumWorkers,
                                   final int numAvailableExtraEvals,
                                   final Map<String, Double> modelParamsMap) {
    final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();
    generateServerPlanForOptimalConfig(serverSummaries, optimalNumServers,
        currNumServers, planBuilder);


    final double avgPullSize = modelParamsMap.get(Constants.AVG_PULL_SIZE_PER_MINI_BATCH);
    generateWorkerPlanForOptimalConfig(avgPullSize, workerSummaries, optimalNumWorkers,
        serverSummaries, optimalNumServers, currNumWorkers, planBuilder);

    planBuilder.setNumAvailableExtraEvaluators(numAvailableExtraEvals > 0 ? numAvailableExtraEvals : 0);

    return planBuilder.build();
  }

  private void generateServerPlanForOptimalConfig(
      final List<EvaluatorSummary> serverSummaries,
      final int optimalNumServers,
      final int activeNumServers,
      final PlanImpl.Builder planBuilder) {

    // assign optimal number of blocks for each server using bandwidth
    final double serverBandwidthSum = serverSummaries.subList(0, optimalNumServers).stream()
        .mapToDouble(EvaluatorSummary::getBandwidth)
        .sum();
    final int totalBlocksInServers = serverSummaries.stream()
        .mapToInt(EvaluatorSummary::getNumBlocks)
        .sum();

    int numAssignedBlocks = 0;

    for (int serverIndex = 0; serverIndex < optimalNumServers; ++serverIndex) {
      final EvaluatorSummary server = serverSummaries.get(serverIndex);

      // the last evaluator takes all remaining blocks
      if (serverIndex == optimalNumServers - 1) {
        server.setNumOptimalBlocks(totalBlocksInServers - numAssignedBlocks);

      } else {
        final int numOptimalBlocks =
            (int) Math.round(totalBlocksInServers * server.bandwidth / serverBandwidthSum);
        numAssignedBlocks += numOptimalBlocks;
        server.setNumOptimalBlocks(numOptimalBlocks);
      }
    }

    if (activeNumServers > optimalNumServers) {
      // delete excess evaluators and un-assign blocks so that data migration plan can be generated accordingly
      for (int serverIndex = optimalNumServers; serverIndex < activeNumServers; ++serverIndex) {
        final EvaluatorSummary server = serverSummaries.get(serverIndex);
        planBuilder.addEvaluatorToDelete(Constants.NAMESPACE_SERVER, server.getId());
        server.setNumOptimalBlocks(0);
      }
    } else if (activeNumServers < optimalNumServers) {
      // add evaluators if necessary
      for (int serverIndex = activeNumServers; serverIndex < optimalNumServers; ++serverIndex) {
        final EvaluatorSummary server = serverSummaries.get(serverIndex);
        planBuilder.addEvaluatorToAdd(Constants.NAMESPACE_SERVER, server.getId());
      }
    }

    generateTransferSteps(Constants.NAMESPACE_SERVER,
        serverSummaries.subList(0, Math.max(optimalNumServers, activeNumServers)),
        planBuilder);
  }

  private void generateWorkerPlanForOptimalConfig(
      final double avgPullSize,
      final List<EvaluatorSummary> workerSummaries,
      final int optimalNumWorkers,
      final List<EvaluatorSummary> serverSummaries,
      final int optimalNumServers,
      final int activeNumWorkers,
      final PlanImpl.Builder planBuilder) {

    final double serverBandwidthSum = serverSummaries.subList(0, optimalNumServers).stream()
        .mapToDouble(EvaluatorSummary::getBandwidth)
        .sum();
    final int totalBlocksInWorkers = workerSummaries.stream()
        .mapToInt(EvaluatorSummary::getNumBlocks)
        .sum();

    final double[] inverseTerms = new double[optimalNumWorkers];
    double termInverseSum = 0D;
    for (int i = 0; i < optimalNumWorkers; i++) {
      inverseTerms[i] = 1 / (1 / workerSummaries.get(i).throughput
          + avgPullSize * Math.max(1 / workerSummaries.get(i).bandwidth, optimalNumWorkers / serverBandwidthSum)
          / miniBatchSize);
      termInverseSum += inverseTerms[i];
    }

    int numAssignedBlocks = 0;

    for (int workerIndex = 0; workerIndex < optimalNumWorkers; ++workerIndex) {
      final EvaluatorSummary worker = workerSummaries.get(workerIndex);

      // the last evaluator takes all remaining blocks
      if (workerIndex == optimalNumWorkers - 1) {
        worker.setNumOptimalBlocks(totalBlocksInWorkers - numAssignedBlocks);

      } else {
        final int numOptimalBlocks =
            (int) Math.round(totalBlocksInWorkers * inverseTerms[workerIndex] / termInverseSum);
        numAssignedBlocks += numOptimalBlocks;
        worker.setNumOptimalBlocks(numOptimalBlocks);
      }
    }

    if (activeNumWorkers > optimalNumWorkers) {
      // delete excess evaluators and un-assign blocks so that data migration plan can be generated accordingly
      for (int workerIndex = optimalNumWorkers; workerIndex < activeNumWorkers; ++workerIndex) {
        final EvaluatorSummary worker = workerSummaries.get(workerIndex);
        planBuilder.addEvaluatorToDelete(Constants.NAMESPACE_WORKER, worker.getId());
        worker.setNumOptimalBlocks(0);
      }
    } else if (activeNumWorkers < optimalNumWorkers) {
      // add evaluators if necessary
      for (int workerIndex = activeNumWorkers; workerIndex < optimalNumWorkers; ++workerIndex) {
        final EvaluatorSummary worker = workerSummaries.get(workerIndex);
        planBuilder.addEvaluatorToAdd(Constants.NAMESPACE_WORKER, worker.getId());
      }
    }

    generateTransferSteps(Constants.NAMESPACE_WORKER,
        workerSummaries.subList(0, Math.max(optimalNumWorkers, activeNumWorkers)),
        planBuilder);
  }

  private List<EvaluatorSummary> sortEvaluatorsByThroughput(
      final List<EvaluatorParameters> params,
      final int availableEvaluators,
      final ToDoubleFunction<EvaluatorParameters> unitCostFunc,
      final ToDoubleFunction<EvaluatorParameters> bandwidthFunc,
      final String newNodeIdPrefix) {

    final double unitCostSum = params.stream()
        .mapToDouble(unitCostFunc)
        .sum();

    final double bandwidthSum = params.stream()
        .mapToDouble(bandwidthFunc)
        .sum();

    final List<EvaluatorSummary> nodes = params.stream()
        .map(param -> new EvaluatorSummary(param.getId(), param.getDataInfo(),
            1 / unitCostFunc.applyAsDouble(param), bandwidthFunc.applyAsDouble(param)))
        .collect(Collectors.toList());

    // sorted in the order of high "throughput"
    Collections.sort(nodes, THROUGHPUT_COMPARATOR);

    final double unitCostAvg = unitCostSum / params.size();
    final double throughput = 1 / unitCostAvg;
    final double bandwidth = bandwidthSum / params.size();

    // We can add up to (availableEvaluators - runningEvaluators - 1) evaluators in each namespace,
    // and reserve at least one evaluator for the other namespace.
    for (int index = 0; index < availableEvaluators - params.size() - 1; ++index) {
      nodes.add(new EvaluatorSummary(newNodeIdPrefix + index, new DataInfoImpl(), throughput, bandwidth));
    }

    return nodes;
  }

  private double totalCost(final int numWorker,
                           final int numTotalDataInstances,
                           final double avgPullSize,
                           final int availableEvaluators,
                           final List<EvaluatorSummary> workers,
                           final List<EvaluatorSummary> servers) {
    final int numServer = availableEvaluators - numWorker;
    double serverBandwidthSum = 0D;
    for (int j = 0; j < numServer; j++) {
      serverBandwidthSum += servers.get(j).bandwidth;
    }

    final double[] terms = new double[numWorker];
    double termInverseSum = 0D;
    for (int i = 0; i < numWorker; i++) {
      terms[i] = 1 / workers.get(i).throughput +
          avgPullSize * Math.max(1 / workers.get(i).bandwidth, numWorker / serverBandwidthSum) / miniBatchSize;
      termInverseSum += 1D / terms[i];
    }

    return numTotalDataInstances / termInverseSum;
  }

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
    private final double bandwidth;
    private int numOptimalBlocks;

    private EvaluatorSummary(final String id, final DataInfo dataInfo,
                             final double throughput, final double bandwidth) {
      this.id = id;
      this.throughput = throughput;
      this.dataInfo = dataInfo;
      this.bandwidth = bandwidth;
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

    public double getBandwidth() {
      return this.bandwidth;
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
          .append(", bandwidth=")
          .append(bandwidth)
          .append(", numOptimalBlocks=")
          .append(numOptimalBlocks)
          .append("]");
      return sb.toString();
    }
  }
}
