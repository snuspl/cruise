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

import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.services.em.common.parameters.AddedEval;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.api.WorkerClock;
import edu.snu.cay.utils.HostnameResolver;
import org.apache.reef.driver.task.TaskConfigurationOptions.Identifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Task for a trainer thread of {@code dolphin-async} applications.
 */
final class AsyncWorkerTask<K, V> implements Task {
  private static final Logger LOG = Logger.getLogger(AsyncWorkerTask.class.getName());
  static final String TASK_ID_PREFIX = "AsyncWorkerTask";

  private final String taskId;
  private final int maxNumEpochs;

  /**
   * Number of training data instances to be processed per mini-batch.
   */
  private final int miniBatchSize;

  private final WorkerSynchronizer synchronizer;
  private final ParameterWorker parameterWorker;
  private final TrainingDataProvider<K, V> trainingDataProvider;
  private final ModelAccessor modelAccessor;
  private final TestDataProvider<V> testDataProvider;
  private final MemoryStore<K> memoryStore;
  private final Trainer<V> trainer;
  private final MetricsMsgSender<WorkerMetrics> metricsMsgSender;
  private final WorkerClock workerClock;
  private final boolean addedEval;
  private final String hostname;

  /**
   * A boolean flag shared among all trainer threads.
   * Trainer threads end when this flag becomes true by {@link #close()}.
   */
  private AtomicBoolean abortFlag = new AtomicBoolean(false);

  @Inject
  private AsyncWorkerTask(@Parameter(Identifier.class) final String taskId,
                          @Parameter(DolphinParameters.MaxNumEpochs.class) final int maxNumEpochs,
                          @Parameter(DolphinParameters.MiniBatchSize.class) final int miniBatchSize,
                          @Parameter(AddedEval.class) final boolean addedEval,
                          final WorkerSynchronizer synchronizer,
                          final ParameterWorker parameterWorker,
                          final TrainingDataProvider<K, V> trainingDataProvider,
                          final ModelAccessor modelAccessor,
                          final TestDataProvider<V> testDataProvider,
                          final MemoryStore<K> memoryStore,
                          final Trainer<V> trainer,
                          final MetricsMsgSender<WorkerMetrics> metricsMsgSender,
                          final WorkerClock workerClock) {
    this.taskId = taskId;
    this.maxNumEpochs = maxNumEpochs;
    this.miniBatchSize = miniBatchSize;
    this.addedEval = addedEval;
    this.synchronizer = synchronizer;
    this.parameterWorker = parameterWorker;
    this.trainingDataProvider = trainingDataProvider;
    this.modelAccessor = modelAccessor;
    this.testDataProvider = testDataProvider;
    this.memoryStore = memoryStore;
    this.trainer = trainer;
    this.metricsMsgSender = metricsMsgSender;
    this.workerClock = workerClock;
    this.hostname = HostnameResolver.resolve();
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "{0} starting...", taskId);

    if (addedEval) {
      LOG.log(Level.INFO, "This worker is added by EM. " +
          "Will skip initializing TrainingDataProvider and Trainer");
    } else {
      // Prepare the training data to be accessible via TrainingDataProvider.
      trainingDataProvider.loadData();

      // TODO #681: Need to add numWorkerThreads concept after multi-thread trainer is enabled
      trainer.initGlobalSettings();
    }

    final List<V> testData = testDataProvider.getTestData();
    LOG.log(Level.INFO, "Test data set size: {0}", testData.size());

    // synchronize all workers before starting the main iterations
    // to avoid meaningless computation by the workers who started earlier
    synchronizer.globalBarrier();

    // initialize the worker clock
    workerClock.initialize();

    final int initialClock = workerClock.getWorkerClock();

    // By starting epochs from the initial clock, which is dynamically fetched from driver,
    // it prevents workers added by EM from starting from epoch 0 and deferring job completion.
    // More specifically, added workers start from the minimum epoch index of other existing workers.
    for (int epochIdx = initialClock; epochIdx < maxNumEpochs; ++epochIdx) {
      LOG.log(Level.INFO, "Starting epoch {0}", epochIdx);
      final long epochStartTime = System.currentTimeMillis();
      final int numEMBlocks = memoryStore.getNumBlocks();
      trainingDataProvider.prepareDataForEpoch();
      parameterWorker.buildAndResetMetrics(); // Reset Tracers in ParameterWorker

      final Collection<V> epochTrainingData = new LinkedList<>();

      int miniBatchIdx = 0;
      while (true) {
        final Collection<V> miniBatchTrainingData = trainingDataProvider.getNextBatchData().values();
        if (miniBatchTrainingData.isEmpty()) {
          break; // Finish the epoch when there are no more data to process
        }
        
        modelAccessor.getAndResetMetrics();
        final long miniBatchStartTime = System.currentTimeMillis();
        trainer.runMiniBatch(miniBatchTrainingData);
        final double miniBatchElapsedTime = (System.currentTimeMillis() - miniBatchStartTime) / 1000.0D;

        sendMiniBatchMetrics(epochIdx, miniBatchIdx, miniBatchTrainingData.size(), miniBatchElapsedTime);

        epochTrainingData.addAll(miniBatchTrainingData);
        miniBatchIdx++;

        if (abortFlag.get()) {
          LOG.log(Level.INFO, "Stop task");
          // record total network waiting time of worker clock when the task is aborted
          workerClock.recordClockNetworkWaitingTime();
          return null;
        }
      }

      final EpochResult epochResult = trainer.onEpochFinished(epochTrainingData, testData, epochIdx);
      final double epochElapsedTime = (System.currentTimeMillis() - epochStartTime) / 1000.0D;

      buildAndSendEpochMetrics(epochResult, epochIdx, miniBatchIdx,
          epochTrainingData.size(), numEMBlocks, epochElapsedTime);

      // TODO #830: Clock should be a unit of mini-batch instead of epoch
      workerClock.clock();
    }

    // Synchronize all workers before cleanup for workers
    // to finish with the globally equivalent view of trained model
    synchronizer.globalBarrier();

    trainer.cleanup();
    // record total network waiting time of worker clock when the task is finished
    workerClock.recordClockNetworkWaitingTime();
    return null;
  }
  
  /**
   * Send mini-batch metrics.
   * @param epochIdx Index of the epoch
   * @param miniBatchIdx Index of the mini-batch
   * @param processedDataItemCount The number of items processed in the mini-batch
   * @param miniBatchElapsedTime Total elapsed time in the mini-batch
   */
  private void sendMiniBatchMetrics(final int epochIdx, final int miniBatchIdx,
                                    final int processedDataItemCount,
                                    final double miniBatchElapsedTime) {
    // Calculate mini-batch computation time by using metrics collected from ModelAccessor
    final Map<String, Double> modelAccessorMetrics = modelAccessor.getAndResetMetrics();
    final double batchPullTime = modelAccessorMetrics.get(ModelAccessor.METRIC_TOTAL_PULL_TIME_SEC);
    final double batchPushTime = modelAccessorMetrics.get(ModelAccessor.METRIC_TOTAL_PUSH_TIME_SEC);
    final double batchCompTime = miniBatchElapsedTime - batchPullTime - batchPushTime;
    final double avgPullTime = modelAccessorMetrics.get(ModelAccessor.METRIC_AVG_PULL_TIME_SEC);
    final double avgPushTime = modelAccessorMetrics.get(ModelAccessor.METRIC_AVG_PUSH_TIME_SEC);
    final double dataProcessingRate = processedDataItemCount / miniBatchElapsedTime;
  
    final WorkerMetrics miniBatchMetric = WorkerMetrics.newBuilder()
        .setDataProcessingRate(dataProcessingRate)
        .setEpochIdx(epochIdx)
        .setMiniBatchIdx(miniBatchIdx)
        .setMiniBatchSize(miniBatchSize)
        .setProcessedDataItemCount(processedDataItemCount)
        .setTotalTime(miniBatchElapsedTime)
        .setTotalCompTime(batchCompTime)
        .setTotalPullTime(batchPullTime)
        .setTotalPushTime(batchPushTime)
        .setAvgPullTime(avgPullTime)
        .setAvgPushTime(avgPushTime)
        .setParameterWorkerMetrics(parameterWorker.buildAndResetMetrics())
        .setHostname(hostname)
        .build();

    LOG.log(Level.INFO, "MiniBatchMetrics {0}", miniBatchMetric);
    metricsMsgSender.send(miniBatchMetric);
  }

  private void buildAndSendEpochMetrics(final EpochResult epochResult,
                                        final int epochIdx, final int miniBatchIdx,
                                        final int processedDataItemCount,
                                        final int numDataBlocks,
                                        final double epochElapsedTime) {
    final WorkerMetrics epochMetric = WorkerMetrics.newBuilder()
        .setMetrics(Metrics.newBuilder().setData(epochResult.getAppMetrics()).build())
        .setEpochIdx(epochIdx)
        .setMiniBatchSize(miniBatchSize)
        .setNumMiniBatchForEpoch(miniBatchIdx)
        .setProcessedDataItemCount(processedDataItemCount)
        .setNumDataBlocks(numDataBlocks)
        .setTotalTime(epochElapsedTime)
        .setHostname(hostname)
        .build();

    LOG.log(Level.INFO, "EpochMetrics {0}", epochMetric);
    metricsMsgSender.send(epochMetric);
  }

  /**
   * Called when the Task is requested to close.
   */
  void close() {
    abortFlag.set(true);
  }
}
