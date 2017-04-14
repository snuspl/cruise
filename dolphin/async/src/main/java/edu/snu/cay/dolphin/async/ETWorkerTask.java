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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.dolphin.async.metric.avro.*;
import edu.snu.cay.services.et.evaluator.impl.MetricCollector;
import org.apache.reef.driver.task.TaskConfigurationOptions.Identifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Task for running Dolphin trainers on ET.
 */
final class ETWorkerTask<K, V> implements Task {
  private static final Logger LOG = Logger.getLogger(ETWorkerTask.class.getName());
  static final String TASK_ID_PREFIX = "ETWorkerTask";

  private final String taskId;
  private final int maxNumEpochs;

  private final WorkerGlobalBarrier workerGlobalBarrier;
  private final TrainingDataProvider<K, V> trainingDataProvider;
  private final Trainer<V> trainer;
  private final MetricCollector<DolphinWorkerMetrics> metricCollector;

  private EpochTime epochTime = new EpochTime();

  @Inject
  private ETWorkerTask(@Parameter(Identifier.class) final String taskId,
                       @Parameter(DolphinParameters.MaxNumEpochs.class) final int maxNumEpochs,
                       final WorkerGlobalBarrier workerGlobalBarrier,
                       final TrainingDataProvider<K, V> trainingDataProvider,
                       final Trainer<V> trainer,
                       final MetricCollector<DolphinWorkerMetrics> metricCollector) {
    this.taskId = taskId;
    this.maxNumEpochs = maxNumEpochs;
    this.workerGlobalBarrier = workerGlobalBarrier;
    this.trainingDataProvider = trainingDataProvider;
    this.trainer = trainer;
    this.metricCollector = metricCollector;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "{0} starting...", taskId);

    trainingDataProvider.loadData();

    trainer.initGlobalSettings();

    // synchronize all workers before starting the main iterations
    // to avoid meaningless computation by the workers who started earlier
    workerGlobalBarrier.await();

    for (int epochIdx = 0; epochIdx < maxNumEpochs; ++epochIdx) {
      LOG.log(Level.INFO, "Starting epoch {0}", epochIdx);
      final long epochStartTime = System.currentTimeMillis();
      trainingDataProvider.prepareDataForEpoch();

      final Collection<V> epochData = new LinkedList<>();

      int miniBatchIdx = 0;
      while (true) {
        final Collection<V> miniBatchData = trainingDataProvider.getNextBatchData().values();
        if (miniBatchData.isEmpty()) {
          break; // Finish the epoch when there are no more data to process
        }

        final long miniBatchStartTime = System.currentTimeMillis();
        final MiniBatchResult miniBatchResult = trainer.runMiniBatch(miniBatchData);
        final double miniBatchElapsedTime = (System.currentTimeMillis() - miniBatchStartTime) / 1000.0D;

        sendBatchMetrics(miniBatchResult, epochIdx, miniBatchIdx,
            miniBatchData.size(), miniBatchElapsedTime);

        epochData.addAll(miniBatchData);
        miniBatchIdx++;
      }

      final EpochResult epochResult = trainer.onEpochFinished(epochData, epochIdx);
      final double epochElapsedTime = (System.currentTimeMillis() - epochStartTime) / 1000.0D;

      sendEpochMetrics(epochResult, epochIdx, miniBatchIdx,
          epochData.size(), epochElapsedTime);
    }

    // Synchronize all workers before cleanup for workers
    // to finish with the globally equivalent view of trained model
    workerGlobalBarrier.await();

    trainer.cleanup();
    return null;
  }

  private void sendBatchMetrics(final MiniBatchResult miniBatchResult,
                                final int epochIdx, final int miniBatchIdx,
                                final int processedDataItemCount,
                                final double miniBatchElapsedTime) {
    final double batchComputeTime = miniBatchResult.getComputeTime();
    final double batchPullTime = miniBatchResult.getTotalPullTime();
    final double batchPushTime = miniBatchResult.getTotalPushTime();
    epochTime.addRecord(batchComputeTime, batchPullTime, batchPushTime);

    final DolphinWorkerMetrics batchMetric = DolphinWorkerMetrics.newBuilder()
        .setType(WorkerMetricsType.BatchMetrics)
        .setBatchMetrics(
            BatchMetrics.newBuilder()
                .setBatchTimeSec(miniBatchElapsedTime)
                .setBatchCustomMetrics(
                    Metrics.newBuilder()
                        .setData(miniBatchResult.getAppMetrics())
                        .build())
                .setNumBatchDataInstances(processedDataItemCount)
                .setBatchIdx(miniBatchIdx)
                .setEpochIdx(epochIdx)
                .setBatchPushTimeSec(batchPushTime)
                .setBatchPullTimeSec(batchPullTime)
                .setBatchCompTimeSec(batchComputeTime)
                .build()
        )
        .build();

    metricCollector.addCustomMetric(batchMetric);
    metricCollector.flush();

    LOG.log(Level.INFO, "MiniBatchMetrics {0}", batchMetric);
  }

  private void sendEpochMetrics(final EpochResult epochResult,
                                final int epochIdx, final int miniBatchIdx,
                                final int processedDataItemCount,
                                final double epochElapsedTime) {
    final DolphinWorkerMetrics epochMetric = DolphinWorkerMetrics.newBuilder()
        .setType(WorkerMetricsType.EpochMetrics)
        .setEpochMetrics(
            EpochMetrics.newBuilder()
                .setEpochCompTimeSec(epochTime.getTotalCompTime())
                .setEpochCustomMetrics(
                    Metrics.newBuilder()
                        .setData(epochResult.getAppMetrics())
                        .build())
                .setEpochIdx(epochIdx)
                .setEpochPullTimeSec(epochTime.getTotalPullTime())
                .setEpochPushTimeSec(epochTime.getTotalPushTime())
                .setEpochTimeSec(epochElapsedTime)
                .setNumBatchesForEpoch(miniBatchIdx)
                .setNumEpochDataInstances(processedDataItemCount)
                .build()
        )
        .build();
    epochTime.reset();

    metricCollector.addCustomMetric(epochMetric);
    metricCollector.flush();

    LOG.log(Level.INFO, "EpochMetrics {0}", epochMetric);
  }

  /**
   * Encapsulates the elapsed time for operations (compute, push, pull) in an epoch.
   */
  private class EpochTime {
    private double totalCompTime;
    private double totalPullTime;
    private double totalPushTime;

    EpochTime() {
      this.totalCompTime = 0d;
      this.totalPullTime = 0d;
      this.totalPushTime = 0d;
    }

    void addRecord(final double compTime, final double pullTime, final double pushTime) {
      totalCompTime += compTime;
      totalPullTime += pullTime;
      totalPushTime += pushTime;
    }

    void reset() {
      this.totalCompTime = 0d;
      this.totalPullTime = 0d;
      this.totalPushTime = 0d;
    }

    private double getTotalCompTime() {
      return totalCompTime;
    }

    private double getTotalPullTime() {
      return totalPullTime;
    }

    private double getTotalPushTime() {
      return totalPushTime;
    }
  }
}
