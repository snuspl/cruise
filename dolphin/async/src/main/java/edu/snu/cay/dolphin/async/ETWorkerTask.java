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
import edu.snu.cay.services.et.metric.MetricCollector;
import org.apache.reef.driver.task.TaskConfigurationOptions.Identifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Task for running Dolphin trainers on ET.
 */
final class ETWorkerTask<V> implements Task {
  private static final Logger LOG = Logger.getLogger(ETWorkerTask.class.getName());
  static final String TASK_ID_PREFIX = "ETWorkerTask";

  private final String taskId;
  private final int startingEpoch;
  private final int maxNumEpochs;

  private final MiniBatchController miniBatchController;
  private final ProgressReporter progressReporter;
  private final WorkerGlobalBarrier workerGlobalBarrier;
  private final TrainingDataProvider<V> trainingDataProvider;
  private final ModelAccessor modelAccessor;
  private final TestDataProvider<V> testDataProvider;
  private final Trainer<V> trainer;
  private final MetricCollector metricCollector;

  /**
   * A boolean flag that becomes true when {@link #close()} is called,
   * which consequently stops the task from training and terminates it.
   */
  private final AtomicBoolean abortFlag = new AtomicBoolean(false);

  @Inject
  private ETWorkerTask(@Parameter(Identifier.class) final String taskId,
                       @Parameter(DolphinParameters.StartingEpochIdx.class) final int startingEpoch,
                       @Parameter(DolphinParameters.MaxNumEpochs.class) final int maxNumEpochs,
                       final MiniBatchController miniBatchController,
                       final ProgressReporter progressReporter,
                       final WorkerGlobalBarrier workerGlobalBarrier,
                       final TrainingDataProvider<V> trainingDataProvider,
                       final ModelAccessor modelAccessor,
                       final TestDataProvider<V> testDataProvider,
                       final Trainer<V> trainer,
                       final MetricCollector metricCollector) {
    this.taskId = taskId;
    this.startingEpoch = startingEpoch;
    this.maxNumEpochs = maxNumEpochs;
    this.miniBatchController = miniBatchController;
    this.progressReporter = progressReporter;
    this.workerGlobalBarrier = workerGlobalBarrier;
    this.trainingDataProvider = trainingDataProvider;
    this.modelAccessor = modelAccessor;
    this.testDataProvider = testDataProvider;
    this.trainer = trainer;
    this.metricCollector = metricCollector;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "{0} starting from epoch {1}", new Object[]{taskId, startingEpoch});

    final List<V> testData = testDataProvider.getTestData();
    LOG.log(Level.INFO, "Test data set size: {0}", testData.size());

    trainer.initGlobalSettings();

    // synchronize all workers before starting the main iterations
    // to avoid meaningless computation by the workers who started earlier
    workerGlobalBarrier.await();

    for (int epochIdx = startingEpoch; epochIdx < maxNumEpochs; ++epochIdx) {
      LOG.log(Level.INFO, "Starting epoch {0}", epochIdx);
      progressReporter.report(epochIdx);

      final long epochStartTime = System.currentTimeMillis();
      final PerOpTimeInEpoch perOpTimeInEpoch = new PerOpTimeInEpoch();
      trainingDataProvider.prepareDataForEpoch();

      final Collection<V> epochData = new LinkedList<>();

      int miniBatchIdx = 0;
      while (true) {
        if (abortFlag.get()) {
          LOG.log(Level.INFO, "The task is getting closed.");
          return null;
        }

        final Collection<V> miniBatchData = trainingDataProvider.getNextBatchData();
        if (miniBatchData.isEmpty()) {
          break; // Finish the epoch when there are no more data to process
        }

        if (!miniBatchController.inquireBatch(epochIdx, miniBatchIdx)) {
          break;
        }

        LOG.log(Level.INFO, "Starting batch {0} in epoch {1}", new Object[] {miniBatchIdx, epochIdx});
        
        modelAccessor.getAndResetMetrics();
        final long miniBatchStartTime = System.currentTimeMillis();
        trainer.runMiniBatch(miniBatchData);
        final double miniBatchElapsedTime = (System.currentTimeMillis() - miniBatchStartTime) / 1000.0D;
        
        sendMiniBatchMetricsAndUpdateEpochOpTime(perOpTimeInEpoch,
            epochIdx, miniBatchIdx, miniBatchData.size(), miniBatchElapsedTime);
        
        epochData.addAll(miniBatchData);
        miniBatchIdx++;

        if (abortFlag.get()) {
          LOG.log(Level.INFO, "The task is getting closed.");
          return null;
        }
      }

      final double epochElapsedTimeSec = (System.currentTimeMillis() - epochStartTime) / 1000.0D;
      final EpochResult epochResult = trainer.onEpochFinished(epochData, testData, epochIdx);
      sendEpochMetrics(epochResult, epochIdx, miniBatchIdx, epochData.size(), epochElapsedTimeSec, perOpTimeInEpoch);
    }

    // Synchronize all workers before cleanup for workers
    // to finish with the globally equivalent view of trained model
    workerGlobalBarrier.await();

    trainer.cleanup();
    return null;
  }
  
  /**
   * Update {@code perOpTimeInEpoch} and send batch metrics.
   * @param perOpTimeInEpoch Update with metrics collected from this mini-batch round
   * @param epochIdx Index of the epoch
   * @param miniBatchIdx Index of the mini-batch
   * @param processedDataItemCount The number of items processed in the epoch
   * @param miniBatchElapsedTime Total elapsed time in this mini-batch round
   */
  private void sendMiniBatchMetricsAndUpdateEpochOpTime(final PerOpTimeInEpoch perOpTimeInEpoch, final int epochIdx,
                                                        final int miniBatchIdx, final int processedDataItemCount,
                                                        final double miniBatchElapsedTime) {
    // Calculate mini-batch computation time by using metrics collected from ModelAccessor
    final Map<String, Double> modelAccessorMetrics = modelAccessor.getAndResetMetrics();
    final double batchPullTime = modelAccessorMetrics.get(ModelAccessor.METRIC_TOTAL_PULL_TIME_SEC);
    final double batchPushTime = modelAccessorMetrics.get(ModelAccessor.METRIC_TOTAL_PUSH_TIME_SEC);
    final double batchCompTime = miniBatchElapsedTime - batchPullTime - batchPushTime;
    final double dataProcessingRate = processedDataItemCount / miniBatchElapsedTime;

    // Update epoch operation time with metrics collected from this mini-batch round
    perOpTimeInEpoch.accumulate(batchCompTime, batchPullTime, batchPushTime);
    
    // Build metrics in the batch
    final BatchMetrics batchMetrics = BatchMetrics.newBuilder()
                .setBatchTimeSec(miniBatchElapsedTime)
                .setDataProcessingRate(dataProcessingRate)
                .setNumBatchDataInstances(processedDataItemCount)
                .setBatchIdx(miniBatchIdx)
                .setEpochIdx(epochIdx)
                .setBatchPushTimeSec(batchPushTime)
                .setBatchPullTimeSec(batchPullTime)
                .setBatchCompTimeSec(batchCompTime)
                .build();

    // Encapsulate the metrics for ET
    final DolphinWorkerMetrics encapsulatedMetrics = DolphinWorkerMetrics.newBuilder()
        .setType(WorkerMetricsType.BatchMetrics)
        .setBatchMetrics(batchMetrics)
        .build();

    metricCollector.addCustomMetric(encapsulatedMetrics);
    metricCollector.flush();
  }

  /**
   * @param epochResult Encapsulates the result of an epoch.
   * @param epochIdx Index of the epoch
   * @param miniBatchIdx Index of the mini-batch
   * @param processedDataItemCount The number of items processed in the epoch
   * @param epochElapsedTime The elapsed time in the epoch in total, including time for computing the objective value.
   * @param perOpTimeInEpoch The elapsed time per operation in the epoch (i.e., computation, pull and push)
   */
  private void sendEpochMetrics(final EpochResult epochResult,
                                final int epochIdx, final int miniBatchIdx,
                                final int processedDataItemCount,
                                final double epochElapsedTime,
                                final PerOpTimeInEpoch perOpTimeInEpoch) {
    // Build App-specific metrics (e.g., Loss, log-likelihood)
    final Metrics appMetrics = Metrics.newBuilder()
        .setData(epochResult.getAppMetrics())
        .build();

    // Build metrics in the epoch
    final EpochMetrics epochMetrics = EpochMetrics.newBuilder()
        .setEpochCompTimeSec(perOpTimeInEpoch.getTotalCompTime())
        .setEpochCustomMetrics(appMetrics)
        .setEpochIdx(epochIdx)
        .setEpochPullTimeSec(perOpTimeInEpoch.getTotalPullTime())
        .setEpochPushTimeSec(perOpTimeInEpoch.getTotalPushTime())
        .setEpochTimeSec(epochElapsedTime)
        .setNumBatchesForEpoch(miniBatchIdx)
        .setNumEpochDataInstances(processedDataItemCount)
        .build();

    // Encapsulate the metrics for ET
    final DolphinWorkerMetrics encapsulatedMetrics = DolphinWorkerMetrics.newBuilder()
        .setType(WorkerMetricsType.EpochMetrics)
        .setEpochMetrics(epochMetrics)
        .build();

    metricCollector.addCustomMetric(encapsulatedMetrics);
    metricCollector.flush();
  }

  /**
   * Called when the Task is requested to close.
   * The {@link #abortFlag} is set true, so the task terminates execution.
   */
  public void close() {
    LOG.log(Level.INFO, "Requested to close!");
    abortFlag.set(true);
  }

  /**
   * Encapsulates the elapsed time per operation (i.e., compute, push, pull) in an epoch.
   */
  private class PerOpTimeInEpoch {
    private double totalCompTime;
    private double totalPullTime;
    private double totalPushTime;

    PerOpTimeInEpoch() {
      this.totalCompTime = 0d;
      this.totalPullTime = 0d;
      this.totalPushTime = 0d;
    }

    /**
     * Accumulate the batch time to compute the total elapsed time per operation in the epoch.
     */
    void accumulate(final double compTime, final double pullTime, final double pushTime) {
      totalCompTime += compTime;
      totalPullTime += pullTime;
      totalPushTime += pushTime;
    }

    double getTotalCompTime() {
      return totalCompTime;
    }

    double getTotalPullTime() {
      return totalPullTime;
    }

    double getTotalPushTime() {
      return totalPushTime;
    }
  }
}
