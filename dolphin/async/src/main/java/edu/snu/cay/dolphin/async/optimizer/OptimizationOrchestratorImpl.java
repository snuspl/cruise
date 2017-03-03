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
import edu.snu.cay.dolphin.async.metric.MetricManager;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.optimizer.parameters.*;
import edu.snu.cay.services.em.driver.api.EMMaster;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToDoubleFunction;
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
                                       @Parameter(NumExtralResources.class) final int numExtraResources,
                                       @Parameter(NumInitialResources.class) final int numInitialResources,
                                       @Parameter(Parameters.LocalRuntimeMaxNumEvaluators.class)
                                         final int localRuntimeMaxNumEvals) {
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
      if (numInitialResources + numExtraResources > localRuntimeMaxNumEvals) {
        LOG.log(Level.WARNING, "The number of resources that optimizer will request exceeds " +
                "the number of evaluators allowed in the local runtime ({0} + {1} > {2}).",
            new Object[] {numInitialResources, numExtraResources, localRuntimeMaxNumEvals});
      }

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
    final List<EvaluatorParameters> processedServerMetrics =
        processMetricsForOptimization(Constants.NAMESPACE_SERVER, currentServerMetrics);
    final List<EvaluatorParameters> processedWorkerMetrics =
        processMetricsForOptimization(Constants.NAMESPACE_WORKER, currentWorkerMiniBatchMetrics);

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
    final double numAvgPullSize = getAvgPullSizePerMiniBatch(currentWorkerMiniBatchMetrics);
    final double numAvgMiniBatch = getAvgNumMiniBatchPerEpoch(currentWorkerEpochMetrics);
    optimizerModelParams.put(Constants.TOTAL_DATA_INSTANCES, (double) numTotalDataInstances);
    optimizerModelParams.put(Constants.AVG_PULL_SIZE_PER_MINI_BATCH, numAvgPullSize);
    optimizerModelParams.put(Constants.AVG_NUM_MINI_BATCH_PER_EPOCH, numAvgMiniBatch);

    final Map<String, List<EvaluatorParameters>> evaluatorParameters = new HashMap<>(2);
    evaluatorParameters.put(Constants.NAMESPACE_SERVER, processedServerMetrics);
    evaluatorParameters.put(Constants.NAMESPACE_WORKER, processedWorkerMetrics);

    final Future future = optimizationThreadPool.submit(new Runnable() {
      @Override
      public void run() {
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
  /**
   * Checks whether the plan is being executed.
   * @return True if the generated plan is on execution
   */
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
   * Calculates the average nubmer of bytes of pull data per mini-batch across workers.
   * @param evalParams a mapping of each worker's ID to the list of {@link EvaluatorParameters}.
   * @return the average number of bytes of pull data per mini-batch across workers.
   */
  private double getAvgPullSizePerMiniBatch(final Map<String, List<EvaluatorParameters>> evalParams) {
    double totalPullData = 0D;
    int count = 0;
    for (final Map.Entry<String, List<EvaluatorParameters>> entry : evalParams.entrySet()) {
      for (final EvaluatorParameters<WorkerMetrics> param : entry.getValue()) {
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

  /**
   * Calculates the exponential moving average value of the collected metrics.
   * This way, recent metrics are given more weight when applied to optimization.
   *
   * Note that this function returns a simple moving average if {@see metricWeightFactor} is 0.
   *
   * @param evalParams a list of {@link EvaluatorParameters}
   * @param targetMetricFunction a function to extract the target metric from {@param evalParams}
   * @return the exponential moving average value
   */
  private double calculateExponentialMovingAverage(final List<EvaluatorParameters> evalParams,
                                                   final ToDoubleFunction<EvaluatorParameters> targetMetricFunction) {
    double weightedSum = 0D;
    double weightedDivisor = 0D;
    final double weightSmoothingFactor = 1.0 - metricWeightFactor;

    // TODO #746: Select an appropriate window size when applying moving averages
    final int metricSubsetSize = (movingAvgWindowSize == 0) ? evalParams.size() : movingAvgWindowSize;

    int metricIdx = 0;
    final ListIterator<EvaluatorParameters> reversedParamsIterator = evalParams.listIterator(evalParams.size());

    while (reversedParamsIterator.hasPrevious() && metricIdx < metricSubsetSize) {
      final double metric = targetMetricFunction.applyAsDouble(reversedParamsIterator.previous());
      final double weight = Math.pow(weightSmoothingFactor, metricIdx);
      weightedSum += weight * metric;
      weightedDivisor += weight;
      metricIdx++;
    }

    return (weightedDivisor == 0.0) ? 0.0 : (weightedSum / weightedDivisor);
  }

  /**
   * Processes raw metrics to extract a representative metric for each evaluator.
   * For servers, the total number of requests and processed times are summed up for average processing time overall.
   * For workers, the average of processing times are to be used.
   * @param namespace
   * @param rawMetrics
   * @return
   */
  // TODO #883: EMA must be applied to the actual performance metric
  private List<EvaluatorParameters> processMetricsForOptimization(
      final String namespace, final Map<String, List<EvaluatorParameters>> rawMetrics) {
    final List<EvaluatorParameters> processedMetrics = new ArrayList<>();

    switch (namespace) {
    case Constants.NAMESPACE_SERVER:
      for (final Map.Entry<String, List<EvaluatorParameters>> entry : rawMetrics.entrySet()) {
        final List<EvaluatorParameters> serverMetric = entry.getValue();
        final ServerMetrics.Builder aggregatedMetricBuilder = ServerMetrics.newBuilder();
        aggregatedMetricBuilder.setTotalPullProcessed(serverMetric.stream().mapToInt(
            param -> ((ServerEvaluatorParameters) param).getMetrics().getTotalPullProcessed()).sum());
        aggregatedMetricBuilder.setTotalPushProcessed(serverMetric.stream().mapToInt(
            param -> ((ServerEvaluatorParameters) param).getMetrics().getTotalPushProcessed()).sum());
        aggregatedMetricBuilder.setTotalPullProcessingTimeSec(serverMetric.stream().mapToDouble(
            param -> ((ServerEvaluatorParameters) param).getMetrics().getTotalPullProcessingTimeSec()).sum());
        aggregatedMetricBuilder.setTotalPushProcessingTimeSec(serverMetric.stream().mapToDouble(
            param -> ((ServerEvaluatorParameters) param).getMetrics().getTotalPushProcessingTimeSec()).sum());

        final ServerMetrics aggregatedMetric = aggregatedMetricBuilder.build();

        // This server did not send metrics meaningful enough for optimization.
        // TODO #862: the following condition may be considered sufficient as Optimization triggering policy changes
        if (aggregatedMetric.getTotalPushProcessed() == 0 || aggregatedMetric.getTotalPullProcessed() == 0) {
          break;
        } else {
          final String serverId = entry.getKey();
          processedMetrics.add(new ServerEvaluatorParameters(serverId,
              new DataInfoImpl((int) calculateExponentialMovingAverage(serverMetric,
                  param -> param.getDataInfo().getNumBlocks())), aggregatedMetric));
        }
      }
      break;
    case Constants.NAMESPACE_WORKER:
      double numTotalKeys = 0;
      for (final Map.Entry<String, List<EvaluatorParameters>> entry : rawMetrics.entrySet()) {
        final List<EvaluatorParameters> workerMetric = entry.getValue();
        final WorkerMetrics.Builder aggregatedMetricBuilder = WorkerMetrics.newBuilder();
        aggregatedMetricBuilder.setProcessedDataItemCount((int) calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getProcessedDataItemCount()));
        aggregatedMetricBuilder.setTotalTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getTotalTime()));
        aggregatedMetricBuilder.setTotalCompTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getTotalCompTime()));
        aggregatedMetricBuilder.setTotalPullTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getTotalPullTime()));
        aggregatedMetricBuilder.setTotalPushTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getTotalPushTime()));
        aggregatedMetricBuilder.setAvgPullTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getAvgPullTime()));
        aggregatedMetricBuilder.setAvgPushTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getAvgPushTime()));

        final WorkerMetrics aggregatedMetric = aggregatedMetricBuilder.build();

        // This worker did not send metrics meaningful enough for optimization.
        if (aggregatedMetric.getProcessedDataItemCount() == 0) {
          break;
        } else {
          final String workerId = entry.getKey();
          processedMetrics.add(new WorkerEvaluatorParameters(workerId,
              new DataInfoImpl((int) calculateExponentialMovingAverage(workerMetric,
                  param -> param.getDataInfo().getNumBlocks())), aggregatedMetric));
        }

        // Estimate the number of keys distributed across servers using the number of pulls from worker-side,
        // as this is used by the optimization model in AsyncDolphinOptimizer.
        numTotalKeys += workerMetric.stream().mapToInt(
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getParameterWorkerMetrics()
                .getTotalPullCount()).average().orElse(0);
      }
      optimizerModelParams.put(Constants.TOTAL_PULLS_PER_MINI_BATCH, numTotalKeys / rawMetrics.size());
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
    return processedMetrics;
  }
}
