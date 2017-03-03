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

import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.function.ToDoubleFunction;

/**
 * A class that process metrics for optimization purpose.
 */
public final class MetricProcessor {

  // utility classes should not be instantiated
  private MetricProcessor() {
  }

  /**
   * Calculates the exponential moving average value of the collected metrics.
   * This way, recent metrics are given more weight when applied to optimization.
   *
   * Note that this function returns a simple moving average if {@see metricWeightFactor} is 0.
   *
   * @param evalParams a list of {@link EvaluatorParameters}
   * @param targetMetricFunction a function to extract the target metric from {@param evalParams}
   * @param metricWeightFactor an exponentially decreasing weight factor for values in EMA
   * @param movingAvgWindowSize moving average window size for applying EMA to the set of collected metrics
   * @return the exponential moving average value
   */
  private static double calculateExponentialMovingAverage(
      final List<EvaluatorParameters> evalParams,
      final ToDoubleFunction<EvaluatorParameters> targetMetricFunction,
      final double metricWeightFactor,
      final int movingAvgWindowSize) {
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
   * @param namespace a namespace that indicates the given metrics are of workers or servers
   * @param rawMetrics metrics to process
   * @param metricWeightFactor an exponentially decreasing weight factor for values in EMA
   * @param movingAvgWindowSize moving average window size for applying EMA to the set of collected metrics
   * @return a processed metrics
   */
  // TODO #883: EMA must be applied to the actual performance metric
  public static List<EvaluatorParameters> processMetricsForOptimization(
      final String namespace,
      final Map<String, List<EvaluatorParameters>> rawMetrics,
      final double metricWeightFactor,
      final int movingAvgWindowSize) {
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
                  param -> param.getDataInfo().getNumBlocks(), metricWeightFactor, movingAvgWindowSize)),
              aggregatedMetric));
        }
      }
      break;
    case Constants.NAMESPACE_WORKER:
      for (final Map.Entry<String, List<EvaluatorParameters>> entry : rawMetrics.entrySet()) {
        final List<EvaluatorParameters> workerMetric = entry.getValue();
        final WorkerMetrics.Builder aggregatedMetricBuilder = WorkerMetrics.newBuilder();
        aggregatedMetricBuilder.setProcessedDataItemCount((int) calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getProcessedDataItemCount(),
            metricWeightFactor, movingAvgWindowSize));
        aggregatedMetricBuilder.setTotalTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getTotalTime(),
            metricWeightFactor, movingAvgWindowSize));
        aggregatedMetricBuilder.setTotalCompTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getTotalCompTime(),
            metricWeightFactor, movingAvgWindowSize));
        aggregatedMetricBuilder.setTotalPullTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getTotalPullTime(),
            metricWeightFactor, movingAvgWindowSize));
        aggregatedMetricBuilder.setTotalPushTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getTotalPushTime(),
            metricWeightFactor, movingAvgWindowSize));
        aggregatedMetricBuilder.setAvgPullTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getAvgPullTime(),
            metricWeightFactor, movingAvgWindowSize));
        aggregatedMetricBuilder.setAvgPushTime(calculateExponentialMovingAverage(workerMetric,
            param -> ((WorkerEvaluatorParameters) param).getMetrics().getAvgPushTime(),
            metricWeightFactor, movingAvgWindowSize));

        final WorkerMetrics aggregatedMetric = aggregatedMetricBuilder.build();

        // This worker did not send metrics meaningful enough for optimization.
        if (aggregatedMetric.getProcessedDataItemCount() == 0) {
          break;
        } else {
          final String workerId = entry.getKey();
          processedMetrics.add(new WorkerEvaluatorParameters(workerId,
              new DataInfoImpl((int) calculateExponentialMovingAverage(workerMetric,
                  param -> param.getDataInfo().getNumBlocks(), metricWeightFactor, movingAvgWindowSize)),
              aggregatedMetric));
        }
      }
      break;
    default:
      throw new RuntimeException("Unsupported namespace");
    }
    return processedMetrics;
  }
}
