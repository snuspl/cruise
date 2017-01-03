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
import edu.snu.cay.dolphin.async.metric.MetricManager;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.optimizer.*;
import edu.snu.cay.dolphin.async.optimizer.SampleOptimizers.*;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.services.em.driver.api.EMMaster;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;

import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.optimizer.parameters.Constants.NAMESPACE_SERVER;
import static edu.snu.cay.dolphin.async.optimizer.parameters.Constants.NAMESPACE_WORKER;

/**
 * Testing purpose implementation of {@link OptimizationOrchestrator}.
 * It invokes various kinds of optimizers to validate the correct working of reconfiguration.
 */
final class TestingOrchestrator implements OptimizationOrchestrator {
  private static final Logger LOG = Logger.getLogger(TestingOrchestrator.class.getName());

  private final PlanExecutor planExecutor;
  private final MetricManager metricManager;

  private final AtomicBoolean isPlanExecuting = new AtomicBoolean(false);

  private final int maxNumEvals;

  private final EMMaster workerEM;
  private final EMMaster serverEM;

  /**
   * A map containing parameters that may be required for the optimization model.
   */
  private final Map<String, Double> optimizerModelParams;

  /**
   * Optimizers to use in test.
   */
  private final Optimizer[] optimizers;

  @Inject
  private TestingOrchestrator(final PlanExecutor planExecutor,
                              final MetricManager metricManager,
                              @Parameter(WorkerEM.class) final EMMaster workerEM,
                              @Parameter(ServerEM.class) final EMMaster serverEM,
                              @Parameter(LocalRuntimeMaxNumEvaluators.class) final int maxNumEvals,
                              final AddOneServerOptimizer addOneServerOptimizer,
                              final AddOneWorkerOptimizer addOneWorkerOptimizer,
                              final DeleteOneServerOptimizer deleteOneServerOptimizer,
                              final DeleteOneWorkerOptimizer deleteOneWorkerOptimizer,
                              final ExchangeOneOptimizer exchangeOneOptimizer) {
    this.planExecutor = planExecutor;
    this.metricManager = metricManager;
    this.workerEM = workerEM;
    this.serverEM = serverEM;
    this.maxNumEvals = maxNumEvals;
    this.optimizerModelParams = new HashMap<>();

    // simply adding a new optimizer in this array will include the optimizer into testing
    // note that job configuration (e.g., running time, timeout, available evaluators) should be adapted
    this.optimizers = new Optimizer[]{
        deleteOneWorkerOptimizer, addOneWorkerOptimizer,
        deleteOneServerOptimizer, addOneServerOptimizer,
        exchangeOneOptimizer};
  }

  private int callsMade = 0;

  /**
   * Runs optimization with optimizers in {@code optimizers} array.
   * It uses one optimizer at a time.
   */
  @Override
  public void run() {
    final int optimizerIdx = callsMade;

    if (optimizerIdx > optimizers.length - 1) {
      LOG.log(Level.INFO, "Optimization testing is done for all sample optimizers");
      return;
    }

    optimizerModelParams.clear();

    // 1) Check that metrics have arrived from all evaluators.
    final Map<String, List<EvaluatorParameters>> currentServerMetrics = metricManager.getServerMetrics();
    final Map<String, List<EvaluatorParameters>> currentWorkerEpochMetrics =
        metricManager.getWorkerEpochMetrics();
    final Map<String, List<EvaluatorParameters>> currentWorkerMiniBatchMetrics =
        metricManager.getWorkerMiniBatchMetrics();

    final int numServerMetricSources = getNumMetricSources(currentServerMetrics);
    final int numWorkerMetricSources = getNumMetricSources(currentWorkerEpochMetrics);
    final int numRunningServers = getNumRunningInstances(serverEM);
    final int numRunningWorkers = getNumRunningInstances(workerEM);

    if (numServerMetricSources < numRunningServers || numWorkerMetricSources < numRunningWorkers) {
      LOG.log(Level.INFO, "Skip this round, because there are missing metrics." +
              " The existing metrics will be kept and reused in the next optimization try." +
              " Metrics from Servers: {0} / {1}, from Workers: {2} / {3}",
          new Object[]{numServerMetricSources, numRunningServers, numWorkerMetricSources, numRunningWorkers});
      // Just return and wait for more metrics to be collected
      return;
    }

    // 2) Process the received metrics (e.g., calculate the EMA of metrics).
    final List<EvaluatorParameters> processedServerMetrics =
        processMetricsForOptimization(Constants.NAMESPACE_SERVER, currentServerMetrics);
    final List<EvaluatorParameters> processedWorkerMetrics =
        processMetricsForOptimization(Constants.NAMESPACE_WORKER, currentWorkerMiniBatchMetrics);

    // 3) Check that the processed metrics suffice to undergo an optimization cycle.
    // processed(*)Metrics of size less that the number of evaluators running in each space implies that
    // there were only metrics not enough for this optimization cycle to be executed.
    if (processedServerMetrics.size() < numRunningServers || processedWorkerMetrics.size() < numRunningWorkers) {
      LOG.log(Level.INFO, "Skip this round, because the metrics do not suffice to undergo an optimization cycle.");
      return;
    }

    // 4) Calculate the total number of data instances distributed across workers,
    // as it can be used by the optimization model.
    final int numTotalDataInstances = getTotalNumDataInstances(Constants.NAMESPACE_WORKER, currentWorkerEpochMetrics);
    optimizerModelParams.put(Constants.TOTAL_DATA_INSTANCES, (double) numTotalDataInstances);

    final Map<String, List<EvaluatorParameters>> evaluatorParameters = new HashMap<>(2);
    evaluatorParameters.put(Constants.NAMESPACE_SERVER, processedServerMetrics);
    evaluatorParameters.put(Constants.NAMESPACE_WORKER, processedWorkerMetrics);

    // 3) run plan from optimizer
    final Optimizer optimizer = optimizers[optimizerIdx];
    executePlan(evaluatorParameters, optimizer);

    isPlanExecuting.set(false);

    callsMade++;
  }

  @Override
  public void onRunningTask(final RunningTask task) {
    planExecutor.onRunningTask(task);
  }

  @Override
  public void onCompletedTask(final CompletedTask task) {
    planExecutor.onCompletedTask(task);
  }

  /**
   * Execute the plan of the chosen optimizer and verify the result.
   * @param evalParams evaluator parameters
   * @param optimizer optimizer
   */
  private void executePlan(final Map<String, List<EvaluatorParameters>> evalParams,
                           final Optimizer optimizer) {
    LOG.log(Level.INFO, "Calculate plan from {0}", optimizer.getClass().getName());
    final Plan plan = optimizer.optimize(evalParams, maxNumEvals, optimizerModelParams);

    // obtain the state of EMs, before executing the plan
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

    metricManager.stopMetricCollection();
    final Future<PlanResult> planResultFuture = planExecutor.execute(plan);
    try {
      final PlanResult result = planResultFuture.get();

      LOG.log(Level.INFO, "The number of executed ops: {0}", result.getNumExecutedOps());

      if (plan.getPlanSize() != result.getNumExecutedOps()) {
        throw new RuntimeException("The number of executed operations is different from the expectation");
      }

      metricManager.loadMetricValidationInfo(workerEM.getEvalIdToNumBlocks(), serverEM.getEvalIdToNumBlocks());
      metricManager.startMetricCollection();

      // obtain the state of EMs, after executing the plan
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

      final Map<String, Integer> serverAddedEvalIdToNumBlocks
          = calculateExpectedResult(beforeServerStoreIdToNumBlocks, NAMESPACE_SERVER, plan);


      final Map<String, Integer> workerAddedEvalIdToNumBlocks
          = calculateExpectedResult(beforeWorkerStoreIdToNumBlocks, NAMESPACE_WORKER, plan);

      verifyResult(beforeServerStoreIdToNumBlocks, serverAddedEvalIdToNumBlocks, afterServerStoreIdToNumBlocks);
      verifyResult(beforeWorkerStoreIdToNumBlocks, workerAddedEvalIdToNumBlocks, afterWorkerStoreIdToNumBlocks);

    } catch (final InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Exception while executing plan", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Calculated the expected result of plan execution.
   * It updates the {@code beforeStoreIdToNumBlocks} by applying the plan virtually.
   * In addition, it returns a separate map representing the number of blocks in each added evaluator,
   * since we don't know which store id has been assigned to added evaluators.
   */
  private Map<String, Integer> calculateExpectedResult(final Map<Integer, Integer> beforeStoreIdToNumBlocks,
                                                       final String namespace,
                                                       final Plan plan) {
    // no change in this namespace
    if (plan.getPlanSize() == 0) {
      return Collections.emptyMap();
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

    return addedEvalIdToNumBlocks;
  }

  /**
   * Verify that the plan is executed correctly by comparing the number of blocks in each EM store.
   * @param expectedExistingStoreIdToNumBlocks block locations before reconfiguration
   * @param actualStoreIdToNumBlocks block locations after reconfiguration
   */
  private void verifyResult(final Map<Integer, Integer> expectedExistingStoreIdToNumBlocks,
                            final Map<String, Integer> expectedAddedEvalIdToNumBlocks,
                            final Map<Integer, Integer> actualStoreIdToNumBlocks) {
    // verify that the plan execution is done correctly
    for (final Map.Entry<Integer, Integer> entry : actualStoreIdToNumBlocks.entrySet()) {
      final int storeId = entry.getKey();
      final int numBlocks = entry.getValue();

      // 1. existing eval
      if (expectedExistingStoreIdToNumBlocks.containsKey(storeId)) {
        if (expectedExistingStoreIdToNumBlocks.get(storeId) != numBlocks) {
          throw new RuntimeException("The number of block in the store " + storeId + " is different from expectation");
        }

        // remove the matched store to check that after-state includes all stores in before-state
        expectedExistingStoreIdToNumBlocks.remove(storeId);

      // 2. newly added eval
      } else {
        String evalId = null;

        // we don't know which store id the new eval has been assigned
        // so, remove one that has the same number of expected blocks
        for (final Map.Entry<String, Integer> innerEntry : expectedAddedEvalIdToNumBlocks.entrySet()) {
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
          expectedAddedEvalIdToNumBlocks.remove(evalId);
        }
      }
    }

    if (!expectedExistingStoreIdToNumBlocks.isEmpty()) {
      throw new RuntimeException("Delete is done to evaluator that should not be");
    }

    if (!expectedAddedEvalIdToNumBlocks.isEmpty()) {
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

  private int getNumRunningInstances(final EMMaster emMaster) {
    return emMaster.getStoreIdToBlockIds().size();
  }

  private int getNumMetricSources(final Map<String, List<EvaluatorParameters>> evalParams) {
    return evalParams.keySet().size();
  }

  /**
   * Calculates the total number of data instances across workers.
   * @param namespace just to check that worker metrics are given
   * @param evalParams a mapping of each worker's ID to the list of {@link EvaluatorParameters}
   *                   in which the first item contains the number of data instances contained in the worker.
   * @return
   */
  private int getTotalNumDataInstances(final String namespace,
                                       final Map<String, List<EvaluatorParameters>> evalParams) {
    int numDataInstances = 0;
    switch (namespace) {
    case Constants.NAMESPACE_WORKER:
      for (final Map.Entry<String, List<EvaluatorParameters>> entry : evalParams.entrySet()) {

        final WorkerEvaluatorParameters firstWorkerEpochMetric = (WorkerEvaluatorParameters) entry.getValue().get(0);
        numDataInstances += firstWorkerEpochMetric.getMetrics().getProcessedDataItemCount();
      }
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
    return numDataInstances;
  }

  /**
   * Processes raw metrics to extract a representative metric for each evaluator.
   * For servers, the total number of requests and processed times are summed up for average processing time overall.
   * For workers, the average of processing times are to be used.
   * @param namespace
   * @param rawMetrics
   * @return
   */
  private List<EvaluatorParameters> processMetricsForOptimization(
      final String namespace, final Map<String, List<EvaluatorParameters>> rawMetrics) {
    final List<EvaluatorParameters> processedMetrics = new ArrayList<>();

    switch (namespace) {
    case Constants.NAMESPACE_SERVER:
      for (final Map.Entry<String, List<EvaluatorParameters>> entry : rawMetrics.entrySet()) {
        final List<EvaluatorParameters> serverMetric = entry.getValue();
        final ServerMetrics.Builder aggregatedMetricBuilder = ServerMetrics.newBuilder();
        aggregatedMetricBuilder.setWindowIndex((int) serverMetric.stream().mapToInt(
            param -> ((ServerMetrics) param.getMetrics()).getWindowIndex()).average().getAsDouble());
        aggregatedMetricBuilder.setMetricWindowMs((int) serverMetric.stream().mapToLong(
            param -> ((ServerMetrics) param.getMetrics()).getMetricWindowMs()).average().getAsDouble());
        aggregatedMetricBuilder.setTotalPullProcessed(serverMetric.stream().mapToInt(
            param -> ((ServerMetrics) param.getMetrics()).getTotalPullProcessed()).sum());
        aggregatedMetricBuilder.setTotalPushProcessed(serverMetric.stream().mapToInt(
            param -> ((ServerMetrics) param.getMetrics()).getTotalPushProcessed()).sum());
        aggregatedMetricBuilder.setTotalPullProcessingTimeSec(serverMetric.stream().mapToDouble(
            param -> ((ServerMetrics) param.getMetrics()).getTotalPullProcessingTimeSec()).sum());
        aggregatedMetricBuilder.setTotalPushProcessingTimeSec(serverMetric.stream().mapToDouble(
            param -> ((ServerMetrics) param.getMetrics()).getTotalPushProcessingTimeSec()).sum());

        final ServerMetrics aggregatedMetric = aggregatedMetricBuilder.build();

        // This server did not send metrics meaningful enough for optimization.
        if (aggregatedMetric.getTotalPushProcessed() == 0 && aggregatedMetric.getTotalPullProcessed() == 0) {
          break;
        } else {
          processedMetrics.add(new ServerEvaluatorParameters(entry.getKey(),
              new DataInfoImpl((int) serverMetric.stream().mapToInt(
                  param -> param.getDataInfo().getNumBlocks()).average().getAsDouble()), aggregatedMetric));
        }
      }
      break;
    case Constants.NAMESPACE_WORKER:
      int numTotalKeys = 0;
      for (final Map.Entry<String, List<EvaluatorParameters>> entry : rawMetrics.entrySet()) {
        final List<EvaluatorParameters> workerMetric = entry.getValue();
        final WorkerMetrics.Builder aggregatedMetricBuilder = WorkerMetrics.newBuilder();
        aggregatedMetricBuilder.setProcessedDataItemCount((int) workerMetric.stream().mapToInt(
            param -> ((WorkerMetrics) param.getMetrics()).getProcessedDataItemCount()).average().getAsDouble());
        aggregatedMetricBuilder.setTotalTime(workerMetric.stream().mapToDouble(
            param -> ((WorkerMetrics) param.getMetrics()).getTotalTime()).average().getAsDouble());
        aggregatedMetricBuilder.setTotalCompTime(workerMetric.stream().mapToDouble(
            param -> ((WorkerMetrics) param.getMetrics()).getTotalCompTime()).average().getAsDouble());
        aggregatedMetricBuilder.setTotalPullTime(workerMetric.stream().mapToDouble(
            param -> ((WorkerMetrics) param.getMetrics()).getTotalPullTime()).average().getAsDouble());
        aggregatedMetricBuilder.setTotalPushTime(workerMetric.stream().mapToDouble(
            param -> ((WorkerMetrics) param.getMetrics()).getTotalPushTime()).average().getAsDouble());
        aggregatedMetricBuilder.setAvgPullTime(workerMetric.stream().mapToDouble(
            param -> ((WorkerMetrics) param.getMetrics()).getAvgPullTime()).average().getAsDouble());
        aggregatedMetricBuilder.setAvgPushTime(workerMetric.stream().mapToDouble(
            param -> ((WorkerMetrics) param.getMetrics()).getAvgPushTime()).average().getAsDouble());

        final WorkerMetrics aggregatedMetric = aggregatedMetricBuilder.build();

        // This worker did not send metrics meaningful enough for optimization.
        if (aggregatedMetric.getTotalCompTime() == 0D) {
          break;
        } else {
          processedMetrics.add(new WorkerEvaluatorParameters(entry.getKey(),
              new DataInfoImpl((int) workerMetric.stream().mapToInt(
                  param -> param.getDataInfo().getNumBlocks()).average().getAsDouble()), aggregatedMetric));
        }

        // Estimate the number of keys distributed across servers using the number of pulls from worker-side,
        // as this is used by the optimization model in AsyncDolphinOptimizer.
        numTotalKeys += workerMetric.stream().mapToInt(
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getParameterWorkerMetrics()
                .getTotalPullCount()).average().orElse(0);
      }
      optimizerModelParams.put(Constants.TOTAL_PULLS_PER_MINI_BATCH, (double) numTotalKeys / rawMetrics.size());
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
    return processedMetrics;
  }
}
