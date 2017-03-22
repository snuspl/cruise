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

import edu.snu.cay.dolphin.async.metric.MetricManager;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.optimizer.parameters.*;
import edu.snu.cay.services.em.driver.api.EMMaster;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Orchestrates the Optimization in Dolphin Async.
 */
public final class OptimizationOrchestratorImpl implements OptimizationOrchestrator {
  private static final Logger LOG = Logger.getLogger(OptimizationOrchestratorImpl.class.getName());

  private final Optimizer optimizer;
  private final PlanExecutor planExecutor;
  private final MetricManager metricManager;

  private final AtomicBoolean isPlanExecuting = new AtomicBoolean(false);
  private final AtomicInteger optimizationCounter = new AtomicInteger(0);

  private final ExecutorService optimizationThreadPool = Executors.newSingleThreadExecutor();

  /**
   * A delay after completion of optimization to wait the system to be stable.
   */
  private final long delayAfterOptimizationMs;

  private volatile int numAvailableEvals;

  /**
   * Weight decreasing factor used for metric EMA.
   */
  private final double metricWeightFactor;

  /**
   * Window size for applying EMA to metrics. This value set to 0 uses all the collected metrics.
   */
  private final int movingAvgWindowSize;

  private final EMMaster workerEMMaster;
  private final EMMaster serverEMMaster;

  /**
   * A map containing parameters that may be required for the optimization model.
   */
  private final Map<String, Double> optimizerModelParams;

  @Inject
  private OptimizationOrchestratorImpl(final Optimizer optimizer,
                                       final PlanExecutor planExecutor,
                                       final MetricManager metricManager,
                                       @Parameter(WorkerEMMaster.class) final EMMaster workerEMMaster,
                                       @Parameter(ServerEMMaster.class) final EMMaster serverEMMaster,
                                       @Parameter(DelayAfterOptimizationMs.class) final long delayAfterOptimizationMs,
                                       @Parameter(MetricWeightFactor.class) final double metricWeightFactor,
                                       @Parameter(MovingAverageWindowSize.class) final int movingAvgWindowSize,
                                       @Parameter(ExtraResourcesPeriodSec.class) final long extraResourcesPeriodSec,
                                       @Parameter(NumExtraResources.class) final int numExtraResources,
                                       @Parameter(NumInitialResources.class) final int numInitialResources) {
    this.optimizer = optimizer;
    this.planExecutor = planExecutor;
    this.metricManager = metricManager;
    this.workerEMMaster = workerEMMaster;
    this.serverEMMaster = serverEMMaster;
    this.delayAfterOptimizationMs = delayAfterOptimizationMs;
    this.metricWeightFactor = metricWeightFactor;
    this.movingAvgWindowSize = movingAvgWindowSize;
    this.optimizerModelParams = new HashMap<>();
    this.numAvailableEvals = numInitialResources;

    // Dynamic resource availability only works if the number of extra resources and its period are set positive.
    if (numExtraResources > 0 && extraResourcesPeriodSec > 0) {
      Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(() -> {
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
  public synchronized void run() {
    // 1) Model params may need to be updated. Simply clear the map, and put the updated values.
    optimizerModelParams.clear();

    // 2) Check that metrics have arrived from all evaluators.
    // Servers: for each window / Workers: for each epoch, but mini-batch metrics are used for the actual optimization.
    final Map<String, List<EvaluatorParameters>> currentServerMetrics = metricManager.getServerMetrics();
    final Map<String, List<EvaluatorParameters>> currentWorkerEpochMetrics =
        metricManager.getWorkerEpochMetrics();
    final Map<String, List<EvaluatorParameters>> currentWorkerMiniBatchMetrics =
        metricManager.getWorkerMiniBatchMetrics();

    // Optimization is skipped if there are missing epoch metrics,
    final int numServerMetricSources = getNumMetricSources(currentServerMetrics);
    final int numWorkerMetricSources = getNumMetricSources(currentWorkerEpochMetrics);
    final int numRunningServers = getNumRunningInstances(serverEMMaster);
    final int numRunningWorkers = getNumRunningInstances(workerEMMaster);

    if (numServerMetricSources < numRunningServers || numWorkerMetricSources < numRunningWorkers) {
      LOG.log(Level.INFO, "Skip this round, because there are missing metrics." +
              " The existing metrics will be kept and reused in the next optimization try." +
              " Metrics from Servers: {0} / {1}, from Workers: {2} / {3}",
          new Object[]{numServerMetricSources, numRunningServers, numWorkerMetricSources, numRunningWorkers});
      // Just return and wait for more metrics to be collected
      return;
    }

    // 3) Process the received metrics (e.g., calculate the EMA of metrics).
    final List<EvaluatorParameters> processedServerMetrics = MetricProcessor.processServerMetrics(
        currentServerMetrics, metricWeightFactor, movingAvgWindowSize);
    final List<EvaluatorParameters> processedWorkerMetrics = MetricProcessor.processWorkerMetrics(
        currentWorkerMiniBatchMetrics, metricWeightFactor, movingAvgWindowSize);

    // 4) Check that the processed metrics suffice to undergo an optimization cycle.
    // processed metrics of size less than the number of evaluators running in each space implies that
    // there were only metrics not enough for this optimization cycle to be executed.
    if (processedServerMetrics.size() < numRunningServers || processedWorkerMetrics.size() < numRunningWorkers) {
      LOG.log(Level.INFO, "Skip this round, because the metrics do not suffice to undergo an optimization cycle.");
      return;
    }

    // 5) Calculate the total number of data instances distributed across workers,
    // as this is used by the optimization model in AsyncDolphinOptimizer.
    final int numTotalDataInstances = getTotalNumDataInstances(currentWorkerEpochMetrics);
    final double numTotalKeys = getTotalPullsPerMiniBatch(currentWorkerMiniBatchMetrics);
    final double numAvgPullSize = getAvgPullSizePerMiniBatch(currentWorkerMiniBatchMetrics);
    final double numAvgMiniBatch = getAvgNumMiniBatchPerEpoch(currentWorkerEpochMetrics);
    optimizerModelParams.put(Constants.TOTAL_DATA_INSTANCES, (double) numTotalDataInstances);
    optimizerModelParams.put(Constants.TOTAL_PULLS_PER_MINI_BATCH, numTotalKeys);
    optimizerModelParams.put(Constants.AVG_PULL_SIZE_PER_MINI_BATCH, numAvgPullSize);
    optimizerModelParams.put(Constants.AVG_NUM_MINI_BATCH_PER_EPOCH, numAvgMiniBatch);

    final Map<String, List<EvaluatorParameters>> evaluatorParameters = new HashMap<>(2);
    evaluatorParameters.put(Constants.NAMESPACE_SERVER, processedServerMetrics);
    evaluatorParameters.put(Constants.NAMESPACE_WORKER, processedWorkerMetrics);

    final Future future = optimizationThreadPool.submit(() -> {
      final int optimizationIdx = optimizationCounter.getAndIncrement();
      LOG.log(Level.INFO, "Start {0}-th optimization", optimizationIdx);
      LOG.log(Level.INFO, "Calculate {0}-th optimal plan with metrics: {1}",
          new Object[]{optimizationIdx, evaluatorParameters});

      // 4) Calculate the optimal plan with the metrics
      final Plan plan;
      try {
        plan = optimizer.optimize(evaluatorParameters, numAvailableEvals, optimizerModelParams);
      } catch (final RuntimeException e) {
        LOG.log(Level.SEVERE, "RuntimeException while calculating the optimal plan", e);
        return;
      }

      // 5) Pause metric collection.
      isPlanExecuting.set(true);
      metricManager.stopMetricCollection();

      // 6) Execute the obtained plan.
      try {
        LOG.log(Level.INFO, "Start executing {0}-th plan: {1}", new Object[]{optimizationIdx, plan});
        final Future<PlanResult> planExecutionResultFuture = planExecutor.execute(plan);
        try {
          final PlanResult planResult = planExecutionResultFuture.get();
          LOG.log(Level.INFO, "Finish {0}-th optimization: {1}", new Object[]{optimizationIdx, planResult});

          Thread.sleep(delayAfterOptimizationMs); // sleep for the system to be stable
        } catch (final InterruptedException | ExecutionException e) {
          LOG.log(Level.WARNING, "Exception while waiting for the plan execution to be completed", e);
        }

      } finally {
        // 7) Once the execution is complete, restart metric collection.
        isPlanExecuting.set(false);
        metricManager.loadMetricValidationInfo(workerEMMaster.getEvalIdToNumBlocks(),
                                               serverEMMaster.getEvalIdToNumBlocks());
        metricManager.startMetricCollection();
      }
    });

    try {
      future.get();
    } catch (final InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Exception while executing optimization", e);
    }
  }

  @Override
  public void onRunningTask(final RunningTask task) {
    planExecutor.onRunningTask(task);
  }

  @Override
  public void onCompletedTask(final CompletedTask task) {
    planExecutor.onCompletedTask(task);
  }

  @Override
  public boolean isPlanExecuting() {
    return isPlanExecuting.get();
  }

  private int getNumRunningInstances(final EMMaster emMaster) {
    return emMaster.getStoreIdToBlockIds().size();
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
