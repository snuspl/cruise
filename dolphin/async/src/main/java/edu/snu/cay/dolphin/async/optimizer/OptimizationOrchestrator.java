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
import edu.snu.cay.dolphin.async.optimizer.parameters.DelayAfterOptimizationMs;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Orchestrates the Optimization in Dolphin Async.
 */
public final class OptimizationOrchestrator {
  private static final Logger LOG = Logger.getLogger(OptimizationOrchestrator.class.getName());

  public static final String NAMESPACE_SERVER = "SERVER";
  public static final String NAMESPACE_WORKER = "WORKER";

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

  private final ElasticMemory workerEM;
  private final ElasticMemory serverEM;

  @Inject
  private OptimizationOrchestrator(final Optimizer optimizer,
                                   final PlanExecutor planExecutor,
                                   final MetricManager metricManager,
                                   @Parameter(WorkerEM.class) final ElasticMemory workerEM,
                                   @Parameter(ServerEM.class) final ElasticMemory serverEM,
                                   @Parameter(DelayAfterOptimizationMs.class) final long delayAfterOptimizationMs,
                                   @Parameter(Parameters.LocalRuntimeMaxNumEvaluators.class) final int maxNumEvals) {
    this.optimizer = optimizer;
    this.planExecutor = planExecutor;
    this.metricManager = metricManager;
    this.workerEM = workerEM;
    this.serverEM = serverEM;
    this.delayAfterOptimizationMs = delayAfterOptimizationMs;
    this.maxNumEvals = maxNumEvals;
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
    // 1) Check that metrics have arrived from all evaluators.
    final Map<String, List<EvaluatorParameters>> currentServerMetrics = metricManager.getServerMetrics();
    final Map<String, List<EvaluatorParameters>> currentWorkerMetrics = metricManager.getWorkerMetrics();

    final int numServerMetricSources = getNumMetricSources(currentServerMetrics);
    final int numWorkerMetricSources = getNumMetricSources(currentWorkerMetrics);
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
        processMetricsForOptimization(NAMESPACE_SERVER, currentServerMetrics);
    final List<EvaluatorParameters> processedWorkerMetrics =
        processMetricsForOptimization(NAMESPACE_WORKER, currentWorkerMetrics);

    // 3) Check that the processed metrics suffice to undergo an optimization cycle.
    // processed(*)Metrics of size less that the number of evaluators running in each space implies that
    // there were only metrics not enough for this optimization cycle to be executed.
    if (processedServerMetrics.size() < numRunningServers || processedWorkerMetrics.size() < numRunningWorkers) {
      LOG.log(Level.INFO, "Skip this round, because the metrics do not suffice to undergo an optimization cycle.");
    }

    final Map<String, List<EvaluatorParameters>> evaluatorParameters = new HashMap<>(2);
    evaluatorParameters.put(NAMESPACE_SERVER, processedServerMetrics);
    evaluatorParameters.put(NAMESPACE_WORKER, processedWorkerMetrics);

    final Future future = optimizationThreadPool.submit(new Runnable() {
      @Override
      public void run() {
        LOG.log(Level.INFO, "Optimization start. Start calculating the optimal plan");

        // 4) Calculate the optimal plan with the metrics
        final Plan plan;
        try {
          plan = optimizer.optimize(evaluatorParameters, maxNumEvals);
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
    return (int) evalParams.entrySet().stream()
        .map(param -> param.getKey())
        .distinct().count();
  }

  /**
   * Processes raw metrics to .
   * For servers, the total number of requests and processed times are summed up for average processing time overall.
   * For workers, the average of processing times are to be used.
   * @param namespace
   * @param rawMetrics
   * @return
   */
  private List<EvaluatorParameters> processMetricsForOptimization(final String namespace,
                                                             final Map<String, List<EvaluatorParameters>> rawMetrics) {
    final List<EvaluatorParameters> processedMetrics = new ArrayList<>();

    switch (namespace) {
    case NAMESPACE_SERVER:
      for (final Map.Entry<String, List<EvaluatorParameters>> entry : rawMetrics.entrySet()) {
        final List<EvaluatorParameters> serverMetric = entry.getValue();
        final ServerEvaluatorParameters firstMetric = (ServerEvaluatorParameters) serverMetric.get(0);
        final ServerMetrics.Builder aggregatedMetricBuilder = ServerMetrics.newBuilder();
        aggregatedMetricBuilder.setWindowIndex(firstMetric.getMetrics().getWindowIndex());
        aggregatedMetricBuilder.setNumModelParamBlocks(firstMetric.getMetrics().getNumModelParamBlocks());
        aggregatedMetricBuilder.setMetricWindowMs(firstMetric.getMetrics().getMetricWindowMs());
        aggregatedMetricBuilder.setTotalPullProcessed(serverMetric.stream().mapToInt(
            param -> ((ServerMetrics) param.getMetrics()).getTotalPullProcessed()).sum());
        aggregatedMetricBuilder.setTotalPushProcessed(serverMetric.stream().mapToInt(
            param -> ((ServerMetrics) param.getMetrics()).getTotalPushProcessed()).sum());
        aggregatedMetricBuilder.setTotalReqProcessed(serverMetric.stream().mapToInt(
            param -> ((ServerMetrics) param.getMetrics()).getTotalReqProcessed()).sum());
        aggregatedMetricBuilder.setTotalPullProcessingTime(serverMetric.stream().mapToDouble(
            param -> ((ServerMetrics) param.getMetrics()).getTotalPullProcessingTime()).sum());
        aggregatedMetricBuilder.setTotalPushProcessingTime(serverMetric.stream().mapToDouble(
            param -> ((ServerMetrics) param.getMetrics()).getTotalPushProcessingTime()).sum());
        aggregatedMetricBuilder.setTotalReqProcessingTime(serverMetric.stream().mapToDouble(
            param -> ((ServerMetrics) param.getMetrics()).getTotalReqProcessingTime()).sum());

        final ServerMetrics aggregatedMetric = aggregatedMetricBuilder.build();

        // This server did not send metrics meaningful enough for optimization.
        if (aggregatedMetric.getTotalPullProcessed() == 0) {
          break;
        } else {
          processedMetrics.add(new ServerEvaluatorParameters(entry.getKey(),
              firstMetric.getDataInfo(), aggregatedMetric));
        }
      }
      break;
    case NAMESPACE_WORKER:
      for (final Map.Entry<String, List<EvaluatorParameters>> entry : rawMetrics.entrySet()) {
        final List<EvaluatorParameters> workerMetric = entry.getValue();
        final WorkerEvaluatorParameters firstMetric = (WorkerEvaluatorParameters) workerMetric.get(0);
        final WorkerMetrics.Builder aggregatedMetricBuilder = WorkerMetrics.newBuilder();
        aggregatedMetricBuilder.setItrIdx(firstMetric.getMetrics().getItrIdx());
        aggregatedMetricBuilder.setNumDataBlocks(firstMetric.getMetrics().getNumDataBlocks());
        aggregatedMetricBuilder.setNumMiniBatchPerItr(firstMetric.getMetrics().getNumMiniBatchPerItr());
        aggregatedMetricBuilder.setProcessedDataItemCount(firstMetric.getMetrics().getProcessedDataItemCount());
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
              firstMetric.getDataInfo(), aggregatedMetric));
        }
      }
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
    return processedMetrics;
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
