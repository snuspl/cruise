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
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
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

  /**
   * Number of training data instances to be processed per mini-batch.
   */
  private final int miniBatchSize;

  private final WorkerGlobalBarrier workerGlobalBarrier;
  private final TrainingDataProvider<K, V> trainingDataProvider;
  private final Trainer<V> trainer;

  @Inject
  private ETWorkerTask(@Parameter(Identifier.class) final String taskId,
                       @Parameter(DolphinParameters.MaxNumEpochs.class) final int maxNumEpochs,
                       @Parameter(DolphinParameters.MiniBatchSize.class) final int miniBatchSize,
                       final WorkerGlobalBarrier workerGlobalBarrier,
                       final TrainingDataProvider<K, V> trainingDataProvider,
                       final Trainer<V> trainer) {
    this.taskId = taskId;
    this.maxNumEpochs = maxNumEpochs;
    this.miniBatchSize = miniBatchSize;
    this.workerGlobalBarrier = workerGlobalBarrier;
    this.trainingDataProvider = trainingDataProvider;
    this.trainer = trainer;
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

        printMiniBatchMetrics(miniBatchResult, epochIdx, miniBatchIdx,
            miniBatchData.size(), miniBatchElapsedTime);

        epochData.addAll(miniBatchData);
        miniBatchIdx++;
      }

      final EpochResult epochResult = trainer.onEpochFinished(epochData, epochIdx);
      final double epochElapsedTime = (System.currentTimeMillis() - epochStartTime) / 1000.0D;

      printEpochMetrics(epochResult, epochIdx, miniBatchIdx,
          epochData.size(), epochElapsedTime);
    }

    // Synchronize all workers before cleanup for workers
    // to finish with the globally equivalent view of trained model
    workerGlobalBarrier.await();

    trainer.cleanup();
    return null;
  }

  private void printMiniBatchMetrics(final MiniBatchResult miniBatchResult,
                                     final int epochIdx, final int miniBatchIdx,
                                     final int processedDataItemCount,
                                     final double miniBatchElapsedTime) {
    final WorkerMetrics miniBatchMetric = WorkerMetrics.newBuilder()
        .setMetrics(Metrics.newBuilder().setData(miniBatchResult.getAppMetrics()).build())
        .setEpochIdx(epochIdx)
        .setMiniBatchIdx(miniBatchIdx)
        .setMiniBatchSize(miniBatchSize)
        .setProcessedDataItemCount(processedDataItemCount)
        .setTotalTime(miniBatchElapsedTime)
        .setTotalCompTime(miniBatchResult.getComputeTime())
        .setTotalPullTime(miniBatchResult.getTotalPullTime())
        .setTotalPushTime(miniBatchResult.getTotalPushTime())
        .setAvgPullTime(miniBatchResult.getAvgPullTime())
        .setAvgPushTime(miniBatchResult.getAvgPushTime())
        .build();

    LOG.log(Level.INFO, "MiniBatchMetrics {0}", miniBatchMetric);
  }

  private void printEpochMetrics(final EpochResult epochResult,
                                 final int epochIdx, final int miniBatchIdx,
                                 final int processedDataItemCount,
                                 final double epochElapsedTime) {
    final WorkerMetrics epochMetric = WorkerMetrics.newBuilder()
        .setMetrics(Metrics.newBuilder().setData(epochResult.getAppMetrics()).build())
        .setEpochIdx(epochIdx)
        .setMiniBatchSize(miniBatchSize)
        .setNumMiniBatchForEpoch(miniBatchIdx)
        .setProcessedDataItemCount(processedDataItemCount)
        .setTotalTime(epochElapsedTime)
        .build();

    LOG.log(Level.INFO, "EpochMetrics {0}", epochMetric);
  }
}
