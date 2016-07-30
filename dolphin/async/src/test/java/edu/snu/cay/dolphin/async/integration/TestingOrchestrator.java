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
package edu.snu.cay.dolphin.async.integration;

import edu.snu.cay.common.param.Parameters.LocalRuntimeMaxNumEvaluators;
import edu.snu.cay.dolphin.async.optimizer.*;
import edu.snu.cay.dolphin.async.optimizer.SampleOptimizers.*;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import edu.snu.cay.services.em.plan.api.TransferStep;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Testing purpose implementation of {@link OptimizationOrchestrator}.
 * It invokes various kinds of optimizers to validate the correct working of reconfiguration.
 */
final class TestingOrchestrator implements OptimizationOrchestrator {
  private static final Logger LOG = Logger.getLogger(TestingOrchestrator.class.getName());

  private final PlanExecutor planExecutor;
  private final MetricsHub metricsHub;

  private final AtomicBoolean isPlanExecuting = new AtomicBoolean(false);

  private final int maxNumEvals;

  private final ElasticMemory workerEM;
  private final ElasticMemory serverEM;

  private final List<EvaluatorParameters> serverParameters = new ArrayList<>();
  private final List<EvaluatorParameters> workerParameters = new ArrayList<>();

  private final Optimizer[] optimizers;

  @Inject
  private TestingOrchestrator(final PlanExecutor planExecutor,
                              final MetricsHub metricsHub,
                              @Parameter(WorkerEM.class) final ElasticMemory workerEM,
                              @Parameter(ServerEM.class) final ElasticMemory serverEM,
                              @Parameter(LocalRuntimeMaxNumEvaluators.class) final int maxNumEvals,
                              final AddOneServerOptimizer addOneServerOptimizer,
                              final AddOneWorkerOptimizer addOneWorkerOptimizer,
                              final DeleteOneServerOptimizer deleteOneServerOptimizer,
                              final DeleteOneWorkerOptimizer deleteOneWorkerOptimizer,
                              final ExchangeOneOptimizer exchangeOneOptimizer) {
    this.planExecutor = planExecutor;
    this.metricsHub = metricsHub;
    this.workerEM = workerEM;
    this.serverEM = serverEM;
    this.maxNumEvals = maxNumEvals;

    this.optimizers = new Optimizer[]{
        deleteOneWorkerOptimizer, addOneWorkerOptimizer,
        deleteOneServerOptimizer, addOneServerOptimizer,
        exchangeOneOptimizer};
  }

  private int callsMade = 0;

  @Override
  public void run() {
    final int optimizerIdx = callsMade;

    if (optimizerIdx > optimizers.length - 1) {
      LOG.log(Level.INFO, "Optimization testing is done for all sample optimizers");
      return;
    }

    // 1) Checks the metric state whether it's enough for the optimization.
    serverParameters.addAll(metricsHub.drainServerMetrics());
    workerParameters.addAll(metricsHub.drainWorkerMetrics());

    final int numServerMetricSources = getNumMetricSources(serverParameters);
    final int numWorkerMetricSources = getNumMetricSources(workerParameters);
    final int numRunningServers = getNumRunningInstances(serverEM);
    final int numRunningWorkers = getNumRunningInstances(workerEM);

    // Case1. If there are metrics from dead nodes
    if (numServerMetricSources > numRunningServers || numWorkerMetricSources > numRunningWorkers) {
      LOG.log(Level.INFO, "Skip this round, because the collected metrics include ones from dead nodes." +
              "The current metrics will be dumped to prevent them from being used in the next optimization try." +
              " Metrics from Servers: {0} / {1}, from Workers: {2} / {3}",
          new Object[]{numServerMetricSources, numRunningServers, numWorkerMetricSources, numRunningWorkers});
      // Dump all the collected metrics
      serverParameters.clear();
      workerParameters.clear();
      return;

      // Case2. If there are missing metrics
    } else if (numServerMetricSources < numRunningServers || numWorkerMetricSources < numRunningWorkers) {
      LOG.log(Level.INFO, "Skip this round, because there are missing metrics." +
              " The existing metrics will be kept and reused in the next optimization try." +
              " Metrics from Servers: {0} / {1}, from Workers: {2} / {3}",
          new Object[]{numServerMetricSources, numRunningServers, numWorkerMetricSources, numRunningWorkers});
      // Just return and wait for more metrics to be collected
      return;
    }

    isPlanExecuting.set(true);

    // 2) Process metrics before starting optimization
    final Map<String, List<EvaluatorParameters>> evaluatorParameters = new HashMap<>(2);

    // use only one latest metric of each evaluator
    final List<EvaluatorParameters> latestServerMetrics = filterLatestParams(serverParameters);
    final List<EvaluatorParameters> latestWorkerMetrics = filterLatestParams(workerParameters);
    serverParameters.clear();
    workerParameters.clear();

    evaluatorParameters.put(Constants.NAMESPACE_SERVER, latestServerMetrics);
    evaluatorParameters.put(Constants.NAMESPACE_WORKER, latestWorkerMetrics);

    // 3) run plan from optimizer
    final Optimizer optimizer = optimizers[optimizerIdx];
    executePlan(evaluatorParameters, optimizer);

    isPlanExecuting.set(false);

    callsMade++;
  }

  /**
   * Execute the plan of the chosen optimizer and verify the result.
   * @param evalParams evaluator parameters
   * @param optimizer optimizer
   */
  private void executePlan(final Map<String, List<EvaluatorParameters>> evalParams,
                           final Optimizer optimizer) {
    LOG.log(Level.INFO, "Calculate plan from {0}", optimizer.getClass().getName());
    final Plan plan = optimizer.optimize(evalParams, maxNumEvals);

    final Map<Integer, Integer> beforeServerStoreIdToNumBlocks = new HashMap<>();
    for (final Map.Entry<Integer, Set<Integer>> entry : serverEM.getStoreIdToBlockIds().entrySet()) {
      final int storeId = entry.getKey();
      final int numBlocks = entry.getValue().size();
      beforeServerStoreIdToNumBlocks.put(storeId, numBlocks);
    }
    final Map<Integer, Integer> beforeWorkerStoreIdToNumBlocks = new HashMap<>();
    for (final Map.Entry<Integer, Set<Integer>> entry : workerEM.getStoreIdToBlockIds().entrySet()) {
      final int storeId = entry.getKey();
      final int numBlocks = entry.getValue().size();
      beforeWorkerStoreIdToNumBlocks.put(storeId, numBlocks);
    }

    final Future<PlanResult> planResultFuture = planExecutor.execute(plan);
    try {
      final PlanResult result = planResultFuture.get();

      LOG.log(Level.INFO, "The number of executed ops: {0}", result.getNumExecutedOps());

      if (plan.getPlanSize() != result.getNumExecutedOps()) {
        throw new RuntimeException("The number of executed operations is different from the expectation");
      }

      final Map<Integer, Integer> afterServerStoreIdToNumBlocks = new HashMap<>();
      for (final Map.Entry<Integer, Set<Integer>> entry : serverEM.getStoreIdToBlockIds().entrySet()) {
        final int storeId = entry.getKey();
        final int numBlocks = entry.getValue().size();
        afterServerStoreIdToNumBlocks.put(storeId, numBlocks);
      }
      final Map<Integer, Integer> afterWorkerStoreIdToNumBlocks = new HashMap<>();
      for (final Map.Entry<Integer, Set<Integer>> entry : workerEM.getStoreIdToBlockIds().entrySet()) {
        final int storeId = entry.getKey();
        final int numBlocks = entry.getValue().size();
        afterWorkerStoreIdToNumBlocks.put(storeId, numBlocks);
      }

      verifyResult(beforeServerStoreIdToNumBlocks, afterServerStoreIdToNumBlocks, Constants.NAMESPACE_SERVER, plan);
      verifyResult(beforeWorkerStoreIdToNumBlocks, afterWorkerStoreIdToNumBlocks, Constants.NAMESPACE_WORKER, plan);

    } catch (final InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Exception while executing plan", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Verify that the plan is executed correctly by comparing the number of blocks in each EM store.
   * @param beforeStoreIdToNumBlocks block locations before reconfiguration
   * @param afterStoreIdToNumBlocks block locations after reconfiguration
   * @param namespace a namespace
   * @param plan a plan
   */
  private void verifyResult(final Map<Integer, Integer> beforeStoreIdToNumBlocks,
                            final Map<Integer, Integer> afterStoreIdToNumBlocks,
                            final String namespace,
                            final Plan plan) {

    // no change in this namespace, so no need to verify
    if (plan.getPlanSize() == 0) {
      return;
    }

    final Collection<String> addedEvals = plan.getEvaluatorsToAdd(namespace);
    final Collection<String> deletedEvals = plan.getEvaluatorsToDelete(namespace);
    final Collection<TransferStep> transferSteps = plan.getTransferSteps(namespace);

    // maintains the number of blocks in the newly added evaluators
    final Map<String, Integer> addedEvalIdToNumBlocks = new HashMap<>();

    // update two maps based on Moves in the plan
    for (final TransferStep transferStep : transferSteps) {
      final String srcEvalId = transferStep.getSrcId();
      final String destEvalId = transferStep.getDstId();
      final int numBlocks = transferStep.getDataInfo().getNumBlocks();

      final int srcNumBlocks = beforeStoreIdToNumBlocks.get(getStoreId(srcEvalId));
      beforeStoreIdToNumBlocks.put(getStoreId(srcEvalId), srcNumBlocks - numBlocks);

      // case 1. destination is added eval
      if (addedEvals.contains(destEvalId)) {
        if (addedEvalIdToNumBlocks.containsKey(destEvalId)) {
          final int destNumBlocks = addedEvalIdToNumBlocks.get(destEvalId);
          addedEvalIdToNumBlocks.put(destEvalId, destNumBlocks + numBlocks);

        } else {
          addedEvalIdToNumBlocks.put(destEvalId, numBlocks);
        }

      // case 2. destination is existing eval
      } else {
        final int destNumBlocks = beforeStoreIdToNumBlocks.get(getStoreId(destEvalId));
        beforeStoreIdToNumBlocks.put(getStoreId(destEvalId), destNumBlocks + numBlocks);
      }
    }

    for (final String evalId : deletedEvals) {
      if (beforeStoreIdToNumBlocks.remove(getStoreId(evalId)) != 0) {
        throw new RuntimeException("Deleted evaluator has blocks more than 0. Optimizer has made wrong plan.");
      }
    }

    // verify that the plan execution is done correctly
    for (final Map.Entry<Integer, Integer> entry : afterStoreIdToNumBlocks.entrySet()) {
      final int storeId = entry.getKey();
      final int numBlocks = entry.getValue();

      // 1. existing eval
      if (beforeStoreIdToNumBlocks.containsKey(storeId)) {
        if (beforeStoreIdToNumBlocks.get(storeId) != numBlocks) {
          throw new RuntimeException("The number of block in the store " + storeId + " is different from expectation");
        }

        // remove the matched store to check that after-state includes all stores in before-state
        beforeStoreIdToNumBlocks.remove(storeId);

      // 2. newly added eval
      } else {
        String evalId = null;

        // we don't know which store id the new eval has been assigned
        // so, remove one that has the same number of expected blocks
        for (final Map.Entry<String, Integer> innerEntry : addedEvalIdToNumBlocks.entrySet()) {
          final String addedEvalId = innerEntry.getKey();
          final int numBlocksForAddedEVal = innerEntry.getValue();

          if (numBlocksForAddedEVal == numBlocks) {
            evalId = addedEvalId;
            break;
          }
        }

        if (evalId == null) {
          throw new RuntimeException("There's no new eval that matches with the expected block number");

        } else {
          // remove matched one
          addedEvalIdToNumBlocks.remove(evalId);
        }
      }
    }

    if (!beforeStoreIdToNumBlocks.isEmpty()) {
      throw new RuntimeException("Delete is done to evaluator that should not be");
    }

    if (!addedEvalIdToNumBlocks.isEmpty()) {
      throw new RuntimeException("Add and Move for the added eval were not correctly done");
    }
  }

  // TODO #509: remove the assumption on the format of contextId
  private int getStoreId(final String evalId) {
    return Integer.valueOf(evalId.split("-")[1]);
  }

  @Override
  public boolean isPlanExecuting() {
    return isPlanExecuting.get();
  }

  private int getNumRunningInstances(final ElasticMemory em) {
    return em.getStoreIdToBlockIds().size();
  }

  private int getNumMetricSources(final List<EvaluatorParameters> evalParams) {
    return (int) evalParams.stream()
        .map(EvaluatorParameters::getId)
        .distinct().count();
  }

  /**
   * Filter the Evaluator Parameters, so that there exist only one latest value per Evaluator.
   */
  private List<EvaluatorParameters> filterLatestParams(final List<EvaluatorParameters> params) {
    final Map<String, EvaluatorParameters> evalIdToParameters = new HashMap<>();
    final ListIterator<EvaluatorParameters> listIterator = params.listIterator(params.size());

    // iterate from the tail in order to save the latest params
    while (listIterator.hasPrevious()) {
      final EvaluatorParameters evalParams = listIterator.previous();
      evalIdToParameters.putIfAbsent(evalParams.getId(), evalParams);
    }

    return new ArrayList<>(evalIdToParameters.values());
  }
}
