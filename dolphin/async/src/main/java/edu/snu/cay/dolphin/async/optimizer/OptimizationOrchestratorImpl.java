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
import edu.snu.cay.dolphin.async.optimizer.parameters.DelayAfterOptimizationMs;
import edu.snu.cay.dolphin.async.optimizer.parameters.MetricWeightFactor;
import edu.snu.cay.dolphin.async.optimizer.parameters.MovingAverageWindowSize;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
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

  private final ExecutorService optimizationThreadPool = Executors.newSingleThreadExecutor();

  /**
   * A delay after completion of optimization to wait the system to be stable.
   */
  private final long delayAfterOptimizationMs;

  private final int maxNumEvals;

  /**
   * Weight decreasing factor used for metric EMA.
   */
  private final double metricWeightFactor;

  /**
   * Window size for applying EMA to metrics. This value set to 0 uses all the collected metrics.
   */
  private final int movingAvgWindowSize;

  private final ElasticMemory workerEM;
  private final ElasticMemory serverEM;

  /**
   * A map containing parameters that may be required for the optimization model.
   */
  private final Map<String, Double> optimizerModelParams;

  @Inject
  private OptimizationOrchestratorImpl(final Optimizer optimizer,
                                   final PlanExecutor planExecutor,
                                   final MetricManager metricManager,
                                   @Parameter(WorkerEM.class) final ElasticMemory workerEM,
                                   @Parameter(ServerEM.class) final ElasticMemory serverEM,
                                   @Parameter(DelayAfterOptimizationMs.class) final long delayAfterOptimizationMs,
                                   @Parameter(MetricWeightFactor.class) final double metricWeightFactor,
                                   @Parameter(MovingAverageWindowSize.class) final int movingAvgWindowSize,
                                   @Parameter(Parameters.LocalRuntimeMaxNumEvaluators.class) final int maxNumEvals) {
    this.optimizer = optimizer;
    this.planExecutor = planExecutor;
    this.metricManager = metricManager;
    this.workerEM = workerEM;
    this.serverEM = serverEM;
    this.delayAfterOptimizationMs = delayAfterOptimizationMs;
    this.metricWeightFactor = metricWeightFactor;
    this.movingAvgWindowSize = movingAvgWindowSize;
    this.maxNumEvals = maxNumEvals;
    this.optimizerModelParams = new HashMap<>();
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
    optimizerModelParams.put(Constants.TOTAL_DATA_INSTANCES, (double) numTotalDataInstances);

    final Map<String, List<EvaluatorParameters>> evaluatorParameters = new HashMap<>(2);
    evaluatorParameters.put(Constants.NAMESPACE_SERVER, processedServerMetrics);
    evaluatorParameters.put(Constants.NAMESPACE_WORKER, processedWorkerMetrics);

    final Future future = optimizationThreadPool.submit(new Runnable() {
      @Override
      public void run() {
        LOG.log(Level.INFO, "Optimization start. Start calculating the optimal plan with metrics: {0}",
            evaluatorParameters);

        // 4) Calculate the optimal plan with the metrics
        final Plan plan;
        try {
          plan = optimizer.optimize(evaluatorParameters, maxNumEvals, optimizerModelParams);
          LOG.log(Level.INFO, "Calculating the optimal plan is finished. Start executing plan: {0}", plan);
        } catch (final RuntimeException e) {
          LOG.log(Level.SEVERE, "RuntimeException while calculating the optimal plan", e);
          return;
        }

        // 5) Pause metric collection.
        isPlanExecuting.set(true);
        metricManager.stopMetricCollection();

        // 6) Execute the obtained plan.
        try {
          final Future<PlanResult> planExecutionResultFuture = planExecutor.execute(plan);
          try {
            final PlanResult planResult = planExecutionResultFuture.get();
            LOG.log(Level.INFO, "Result of plan execution: {0}", planResult);

            Thread.sleep(delayAfterOptimizationMs); // sleep for the system to be stable
          } catch (final InterruptedException | ExecutionException e) {
            LOG.log(Level.WARNING, "Exception while waiting for the plan execution to be completed", e);
          }

        } finally {
          // 7) Once the execution is complete, restart metric collection.
          isPlanExecuting.set(false);
          metricManager.loadMetricValidationInfo(workerEM.getEvalIdToNumBlocks(), serverEM.getEvalIdToNumBlocks());
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

  private int getNumRunningInstances(final ElasticMemory em) {
    return em.getStoreIdToBlockIds().size();
  }

  private int getNumMetricSources(final Map<String, List<EvaluatorParameters>> evalParams) {
    return evalParams.keySet().size();
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
      int numTotalKeys = 0;
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
      optimizerModelParams.put(Constants.TOTAL_PULLS_PER_MINI_BATCH, (double) numTotalKeys / rawMetrics.size());
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
    return processedMetrics;
  }
}
