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
package edu.snu.cay.dolphin.async.optimizer.impl;

import edu.snu.cay.dolphin.async.ETModelAccessor;
import edu.snu.cay.dolphin.async.ETTrainingDataProvider;
import edu.snu.cay.dolphin.async.WorkerStateManager;
import edu.snu.cay.dolphin.async.ETTaskRunner;
import edu.snu.cay.dolphin.async.metric.MetricManager;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.optimizer.api.EvaluatorParameters;
import edu.snu.cay.dolphin.async.optimizer.api.Optimizer;
import edu.snu.cay.dolphin.async.optimizer.parameters.*;
import edu.snu.cay.dolphin.async.plan.impl.PlanCompiler;
import edu.snu.cay.dolphin.async.plan.api.Plan;
import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.services.et.plan.api.PlanExecutor;
import edu.snu.cay.services.et.plan.impl.ETPlan;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Orchestrates the Optimization in Dolphin on ET.
 */
public final class ETOptimizationOrchestrator {
  private static final Logger LOG = Logger.getLogger(ETOptimizationOrchestrator.class.getName());

  private final Optimizer optimizer;

  private final ETMaster etMaster;

  private final PlanExecutor planExecutor;

  private final PlanCompiler planCompiler;

  private final MetricManager metricManager;

  private final WorkerStateManager workerStateManager;

  private final ETTaskRunner taskRunner;

  private final AtomicInteger optimizationCounter = new AtomicInteger(0);

  private final ExecutorService optimizationThreadPool = Executors.newSingleThreadExecutor();

  private final long optimizationIntervalMs;

  /**
   * A delay after completion of optimization to wait the system to be stable.
   */
  private final long delayAfterOptimizationMs;

  private volatile int numAvailableEvals;

  private final double metricWeightFactor;

  private final int movingAverageWindowSize;

  @Inject
  private ETOptimizationOrchestrator(final Optimizer optimizer,
                                     final ETMaster etMaster,
                                     final PlanExecutor planExecutor,
                                     final MetricManager metricManager,
                                     final PlanCompiler planCompiler,
                                     final ETTaskRunner taskRunner,
                                     final WorkerStateManager workerStateManager,
                                     @Parameter(OptimizationIntervalMs.class) final long optimizationIntervalMs,
                                     @Parameter(DelayAfterOptimizationMs.class) final long delayAfterOptimizationMs,
                                     @Parameter(ExtraResourcesPeriodSec.class) final long extraResourcesPeriodSec,
                                     @Parameter(NumExtraResources.class) final int numExtraResources,
                                     @Parameter(NumInitialResources.class) final int numInitialResources,
                                     @Parameter(MetricWeightFactor.class) final double metricWeightFactor,
                                     @Parameter(MovingAverageWindowSize.class) final int movingAverageWindowSize) {
    this.optimizer = optimizer;
    this.planExecutor = planExecutor;
    this.planCompiler = planCompiler;
    this.metricManager = metricManager;
    this.etMaster = etMaster;
    this.taskRunner = taskRunner;
    this.workerStateManager = workerStateManager;
    this.optimizationIntervalMs = optimizationIntervalMs;
    this.delayAfterOptimizationMs = delayAfterOptimizationMs;
    this.numAvailableEvals = numInitialResources;
    this.metricWeightFactor = metricWeightFactor;
    this.movingAverageWindowSize = movingAverageWindowSize;
    
    // Dynamic resource availability only works if the number of extra resources and its period are set positive.
    if (numExtraResources > 0 && extraResourcesPeriodSec > 0) {
      Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
        if (numAvailableEvals == numInitialResources) {
          this.numAvailableEvals = numInitialResources + numExtraResources;
        } else {
          this.numAvailableEvals = numInitialResources;
        }
        LOG.log(Level.INFO, "The number of available resources has changed to {0}", numAvailableEvals);
      }, extraResourcesPeriodSec, extraResourcesPeriodSec, TimeUnit.SECONDS);
    } else if (numExtraResources < 0 || extraResourcesPeriodSec < 0) {
      final String msg = String.format("Both the number of extra resources and the period should be set positive. " +
          "But given (%d, %d) respectively", numExtraResources, extraResourcesPeriodSec);
      throw new RuntimeException(msg);
    } else {
      LOG.log(Level.INFO, "The number of resources is fixed to {0}", numInitialResources);
    }
  }

  /**
   * Start optimization in background.
   */
  public void start() {
    Executors.newSingleThreadExecutor().submit(() -> {
      try {
        workerStateManager.waitWorkersToFinishInitStage();

        final AllocatedTable modelTable;
        final AllocatedTable inputTable;
        try {
          modelTable = etMaster.getTable(ETModelAccessor.MODEL_TABLE_ID);
          inputTable = etMaster.getTable(ETTrainingDataProvider.TRAINING_DATA_TABLE_ID);
        } catch (TableNotExistException e) {
          throw new RuntimeException(e);
        }

        metricManager.loadMetricValidationInfo(
            getValidationInfo(inputTable.getPartitionInfo()), getValidationInfo(modelTable.getPartitionInfo()));
        metricManager.startMetricCollection();

        while (workerStateManager.tryEnterOptimization()) {
          final Map<String, Pair<Set<String>, Set<String>>> namespaceToexecutorChange = optimize();

          final Pair<Set<String>, Set<String>> changesInServers =
              namespaceToexecutorChange.get(Constants.NAMESPACE_SERVER);

          final Pair<Set<String>, Set<String>> changesInWorkers =
              namespaceToexecutorChange.get(Constants.NAMESPACE_WORKER);

          // should notify taskRunner first
          taskRunner.updateExecutorEntry(changesInWorkers.getLeft(), changesInWorkers.getRight(),
              changesInServers.getLeft(), changesInServers.getRight());
          workerStateManager.onOptimizationFinished(changesInWorkers.getLeft(), changesInWorkers.getRight());

          try {
            LOG.log(Level.INFO, "Sleep {0} ms for next optimization", optimizationIntervalMs);
            Thread.sleep(optimizationIntervalMs);
          } catch (InterruptedException e) {
            LOG.log(Level.WARNING, "Interrupted while sleeping for next optimization try." +
                " Let's try optimization now.", e);
          }
        }

        metricManager.stopMetricCollection();
      } catch (Exception e) {
        LOG.log(Level.INFO, "Exception in optimization thread", e);
      }
    });
  }

  /**
   * Runs optimization based on the metrics from {@link MetricManager} in the following steps.
   * 1) Check that metrics have arrived from all evaluators.
   * 2) Process the received metrics (e.g., calculate the EMA of metrics).
   *    Here, we assume that invalid metrics have already been considered in {@link MetricManager}.
   * 3) Check that the processed metrics suffice to undergo an optimization cycle.
   * 4) Calculate the optimal plan with the metrics.
   * 5) Pause metric collection.
   * 6) Execute the obtained plan.
   * 7) Once the execution is complete, restart metric collection.
   */
  private synchronized Map<String, Pair<Set<String>, Set<String>>> optimize() {
    final Map<String, Pair<Set<String>, Set<String>>> emptyResult = new HashMap<>();
    emptyResult.put(Constants.NAMESPACE_WORKER, Pair.of(Collections.emptySet(), Collections.emptySet()));
    emptyResult.put(Constants.NAMESPACE_SERVER, Pair.of(Collections.emptySet(), Collections.emptySet()));

    // 1) Check that metrics have arrived from all evaluators.
    // Servers: for each window / Workers: for each epoch, but mini-batch metrics are used for the actual optimization.
    final Map<String, List<EvaluatorParameters>> currentServerMetrics = metricManager.getServerMetrics();
    final Map<String, List<EvaluatorParameters>> currentWorkerEpochMetrics =
        metricManager.getWorkerEpochMetrics();
    final Map<String, List<EvaluatorParameters>> currentWorkerMiniBatchMetrics =
        metricManager.getWorkerMiniBatchMetrics();

    final AllocatedTable modelTable;
    final AllocatedTable inputTable;
    try {
      modelTable = etMaster.getTable(ETModelAccessor.MODEL_TABLE_ID);
      inputTable = etMaster.getTable(ETTrainingDataProvider.TRAINING_DATA_TABLE_ID);
    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }

    // Optimization is skipped if there are missing epoch metrics,
    final int numServerMetricSources = getNumMetricSources(currentServerMetrics);
    final int numWorkerMetricSources = getNumMetricSources(currentWorkerEpochMetrics);

    final int numRunningServers = modelTable.getPartitionInfo().size();
    final int numRunningWorkers = inputTable.getPartitionInfo().size();

    if (numServerMetricSources < numRunningServers || numWorkerMetricSources < numRunningWorkers) {
      LOG.log(Level.INFO, "Skip this round, because there are missing metrics." +
              " The existing metrics will be kept and reused in the next optimization try." +
              " Metrics from Servers: {0} / {1}, from Workers: {2} / {3}",
          new Object[]{numServerMetricSources, numRunningServers, numWorkerMetricSources, numRunningWorkers});
      // Just return and wait for more metrics to be collected
      return emptyResult;
    }

    // 2) Process the received metrics (e.g., calculate the EMA of metrics).
    final List<EvaluatorParameters> processedServerMetrics =
        MetricProcessor.processServerMetrics(currentServerMetrics, metricWeightFactor, movingAverageWindowSize);

    final List<EvaluatorParameters> processedWorkerMetrics = MetricProcessor.processWorkerMetrics(
        currentWorkerMiniBatchMetrics, metricWeightFactor, movingAverageWindowSize);

    // 3) Check that the processed metrics suffice to undergo an optimization cycle.
    // processed metrics of size less than the number of evaluators running in each space implies that
    // there were only metrics not enough for this optimization cycle to be executed.
    if (processedServerMetrics.size() < numRunningServers || processedWorkerMetrics.size() < numRunningWorkers) {
      LOG.log(Level.INFO, "Skip this round, because the metrics do not suffice to undergo an optimization cycle.");
      return emptyResult;
    }

    // Calculate the total number of data instances distributed across workers,
    // as this is used by the optimization model in AsyncDolphinOptimizer.
    final int numTotalDataInstances = getTotalNumDataInstances(currentWorkerEpochMetrics);
    final double numTotalKeys = getTotalPullsPerMiniBatch(currentWorkerMiniBatchMetrics);
    final double numAvgPullSize = getAvgPullSizePerMiniBatch(currentWorkerMiniBatchMetrics);
    final double numAvgMiniBatch = getAvgNumMiniBatchPerEpoch(currentWorkerEpochMetrics);

    // A map containing additional parameters for optimizer.
    final Map<String, Double> optimizerModelParams = new HashMap<>();
    optimizerModelParams.put(Constants.TOTAL_DATA_INSTANCES, (double) numTotalDataInstances);
    optimizerModelParams.put(Constants.TOTAL_PULLS_PER_MINI_BATCH, numTotalKeys);
    optimizerModelParams.put(Constants.AVG_PULL_SIZE_PER_MINI_BATCH, numAvgPullSize);
    optimizerModelParams.put(Constants.AVG_NUM_MINI_BATCH_PER_EPOCH, numAvgMiniBatch);

    final Map<String, List<EvaluatorParameters>> evaluatorParameters = new HashMap<>(2);
    evaluatorParameters.put(Constants.NAMESPACE_SERVER, processedServerMetrics);
    evaluatorParameters.put(Constants.NAMESPACE_WORKER, processedWorkerMetrics);

    final Future<Map<String, Pair<Set<String>, Set<String>>>> future = optimizationThreadPool.submit(() -> {
      final int optimizationCount = optimizationCounter.getAndIncrement();
      LOG.log(Level.INFO, "Start {0}-th optimization", optimizationCount);
      LOG.log(Level.INFO, "Calculate {0}-th optimal plan with metrics: {1}",
          new Object[]{optimizationCount, evaluatorParameters});

      // 4) Calculate the optimal plan with the metrics
      final Plan plan;
      try {
        plan = optimizer.optimize(evaluatorParameters, numAvailableEvals, optimizerModelParams);
      } catch (final RuntimeException e) {
        LOG.log(Level.SEVERE, "RuntimeException while calculating the optimal plan", e);
        return emptyResult;
      }

      // 5) Pause metric collection.
      metricManager.stopMetricCollection();

      final ETPlan etPlan = planCompiler.compile(plan, numAvailableEvals);

      final Set<String> workersBeforeOpt = new HashSet<>(inputTable.getPartitionInfo().keySet());
      final Set<String> serversBeforeOpt = new HashSet<>(modelTable.getPartitionInfo().keySet());

      // 6) Execute the obtained plan.
      try {
        LOG.log(Level.INFO, "Start executing {0}-th plan: {1}", new Object[]{optimizationCount, plan});

        final ListenableFuture<?> planExecutionResultFuture = planExecutor.execute(etPlan);
        try {
          planExecutionResultFuture.get();
          LOG.log(Level.INFO, "Finish executing {0}-th plan: {1}", new Object[]{optimizationCount, plan});

          final Set<String> workersAfterOpt = new HashSet<>(inputTable.getPartitionInfo().keySet());
          final Set<String> serversAfterOpt = new HashSet<>(modelTable.getPartitionInfo().keySet());

          final Set<String> addedWorkers = new HashSet<>(workersAfterOpt);
          addedWorkers.removeAll(workersBeforeOpt);
          final Set<String> addedServers = new HashSet<>(serversAfterOpt);
          addedServers.removeAll(serversBeforeOpt);

          final Set<String> deletedWorkers = new HashSet<>(workersBeforeOpt);
          deletedWorkers.removeAll(workersAfterOpt);
          final Set<String> deletedServers = new HashSet<>(serversBeforeOpt);
          deletedServers.removeAll(serversAfterOpt);

          // sleep for the system to be stable after executing a plan
          Thread.sleep(delayAfterOptimizationMs);


          final Map<String, Pair<Set<String>, Set<String>>> namespaceToExecutorChanges = new HashMap<>();
          namespaceToExecutorChanges.put(Constants.NAMESPACE_WORKER, Pair.of(addedWorkers, deletedWorkers));
          namespaceToExecutorChanges.put(Constants.NAMESPACE_SERVER, Pair.of(addedServers, deletedServers));

          return namespaceToExecutorChanges;

        } catch (final InterruptedException | ExecutionException e) {
          throw new RuntimeException("Exception while waiting for the plan execution to be completed", e);
        }

      } finally {
        // 7) Once the execution is complete, restart metric collection.
        metricManager.loadMetricValidationInfo(
            getValidationInfo(inputTable.getPartitionInfo()), getValidationInfo(modelTable.getPartitionInfo()));
        metricManager.startMetricCollection();
      }
    });

    try {
      return future.get();
    } catch (final InterruptedException | ExecutionException e) {
      throw new RuntimeException("Exception while executing optimization", e);
    }
  }

  private Map<String, Integer> getValidationInfo(final Map<String, Set<Integer>> executorToBlocks) {
    final Map<String, Integer> executorToNumBlocks = new HashMap<>();
    executorToBlocks.forEach((executor, blocks) -> executorToNumBlocks.put(executor, blocks.size()));
    return executorToNumBlocks;
  }

  private int getNumMetricSources(final Map<String, List<EvaluatorParameters>> evalParams) {
    int validMetricSources = 0;
    for (final Map.Entry<String, List<EvaluatorParameters>> entry : evalParams.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        validMetricSources++;
      }
    }
    return validMetricSources;
  }

  /**
   * Calculates the total number of data instances across workers.
   * @param evalParams a mapping of each worker's ID to the list of {@link EvaluatorParameters}
   *                   in which the first item contains the number of data instances contained in the worker.
   * @return the total number of data instances across workers.
   */
  private int getTotalNumDataInstances(final Map<String, List<EvaluatorParameters>> evalParams) {
    int numDataInstances = 0;
    for (final Map.Entry<String, List<EvaluatorParameters>> entry : evalParams.entrySet()) {

      final WorkerEvaluatorParameters firstWorkerEpochMetric = (WorkerEvaluatorParameters) entry.getValue().get(0);
      numDataInstances += firstWorkerEpochMetric.getMetrics().getProcessedDataItemCount();
    }
    return numDataInstances;
  }

  /**
   * Calculates the total number of pulls per mini-batch from each worker,
   * by summing pull counts from all workers and dividing it by the number of workers.
   * @param evalParams a mapping of each worker's ID to the list of {@link EvaluatorParameters}.
   * @return the total number of pulls per mini-batch from each worker.
   */
  private double getTotalPullsPerMiniBatch(final Map<String, List<EvaluatorParameters>> evalParams) {
    int numTotalKeys = 0;
    for (final List<EvaluatorParameters> workerMetrics : evalParams.values()) {
      // Estimate the number of keys distributed across servers using the number of pulls from worker-side,
      // as this is used by the optimization model in AsyncDolphinOptimizer.
      numTotalKeys += workerMetrics.stream().mapToInt(
          param -> ((WorkerEvaluatorParameters) param).getMetrics().getParameterWorkerMetrics()
              .getTotalPullCount()).average().orElse(0);
    }

    return numTotalKeys / evalParams.size();
  }

  /**
   * Calculates the average number of bytes of pull data per mini-batch across workers.
   * @param evalParams a mapping of each worker's ID to the list of {@link EvaluatorParameters}.
   * @return the average number of bytes of pull data per mini-batch across workers.
   */
  private double getAvgPullSizePerMiniBatch(final Map<String, List<EvaluatorParameters>> evalParams) {
    double totalPullData = 0D;
    int count = 0;
    for (final List<EvaluatorParameters> evalParamList : evalParams.values()) {
      for (final EvaluatorParameters<WorkerMetrics> param : evalParamList) {
        // do not include mini-batches which processed data less than mini-batch size
        if ((int) param.getMetrics().getMiniBatchSize() == param.getMetrics().getProcessedDataItemCount()) {
          totalPullData += param.getMetrics().getParameterWorkerMetrics().getTotalReceivedBytes();
          count++;
        }
      }
    }
    return totalPullData / count;
  }

  private double getAvgNumMiniBatchPerEpoch(final Map<String, List<EvaluatorParameters>> evalParams) {
    return evalParams.entrySet().stream().mapToDouble(entry ->
        ((WorkerMetrics) entry.getValue().get(0).getMetrics()).getNumMiniBatchForEpoch()).average().orElse(0D);
  }
}
