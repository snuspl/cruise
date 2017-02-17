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
package edu.snu.cay.dolphin.async;

import  edu.snu.cay.common.param.Parameters.Iterations;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.ps.worker.api.WorkerClock;
import org.apache.reef.driver.task.TaskConfigurationOptions.Identifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Task for a trainer thread of {@code dolphin-async} applications.
 */
@Unit
final class AsyncWorkerTask<K, V> implements Task {
  private static final Logger LOG = Logger.getLogger(AsyncWorkerTask.class.getName());
  static final String TASK_ID_PREFIX = "AsyncWorkerTask";

  private final String taskId;
  private final int maxIterations;
  private final WorkerSynchronizer synchronizer;
  private final TrainingDataProvider<K, V> trainingDataProvider;
  private final MemoryStore<K> memoryStore;
  private final Trainer<V> trainer;
  private final WorkerClock workerClock;

  /**
   * A boolean flag shared among all trainer threads.
   * Trainer threads end when this flag becomes true by {@link #close()}.
   */
  private AtomicBoolean abortFlag = new AtomicBoolean(false);

  @Inject
  private AsyncWorkerTask(@Parameter(Identifier.class) final String taskId,
                          @Parameter(Iterations.class) final int maxIterations,
                          final WorkerSynchronizer synchronizer,
                          final TrainingDataProvider<K, V> trainingDataProvider,
                          final MemoryStore<K> memoryStore,
                          final Trainer<V> trainer,
                          final WorkerClock workerClock) {
    this.taskId = taskId;
    this.maxIterations = maxIterations;
    this.synchronizer = synchronizer;
    this.trainingDataProvider = trainingDataProvider;
    this.memoryStore = memoryStore;
    this.trainer = trainer;
    this.workerClock = workerClock;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "{0} starting...", taskId);

    // Prepare the training data to be accessible via TrainingDataProvider.
    trainingDataProvider.initialize();

    // TODO #681: Need to add numWorkerThreads concept after multi-thread trainer is enabled
    trainer.initialize();

    // synchronize all workers before starting the main iteration
    // to avoid meaningless iterations by the workers who started earlier
    synchronizer.globalBarrier();

    // initialize the worker clock
    workerClock.initialize();

    final int initialClock = workerClock.getWorkerClock();

    // By starting iteration from the initial clock, which is dynamically fetched from driver,
    // it prevents workers added by EM from starting from iteration 0 and deferring job completion.
    // More specifically, added workers start from the minimum iteration of other existing workers.
    for (int iteration = initialClock; iteration < maxIterations; ++iteration) {
      LOG.log(Level.INFO, "Starting iteration {0}", iteration);
      final long epochStartTime = System.currentTimeMillis();
      final int numEMBlocks = memoryStore.getNumBlocks();
      trainingDataProvider.prepareDataForEpoch();

      final Collection<V> epochData = new LinkedList<>();

      int miniBatchIdx = 0;
      while (true) {
        final Collection<V> miniBatchData = trainingDataProvider.getNextTrainingData().values();
        if (miniBatchData.isEmpty()) {
          break; // Finish the epoch when there are no more data to process
        }

        final MiniBatchInfo miniBatchInfo = MiniBatchInfo.getBuilder()
            .setEpochIdx(iteration)
            .setMiniBatchIdx(miniBatchIdx)
            .build();

        trainer.runMiniBatch(miniBatchData, miniBatchInfo);
        epochData.addAll(miniBatchData);
        miniBatchIdx++;

        if (abortFlag.get()) {
          LOG.log(Level.INFO, "Stop task");
          // record total network waiting time of worker clock when the task is aborted
          workerClock.recordClockNetworkWaitingTime();
          return null;
        }
      }

      final EpochInfo epochInfo = EpochInfo.getBuilder()
              .setEpochIdx(iteration)
              .setNumMiniBatches(miniBatchIdx)
              .setNumEMBlocks(numEMBlocks)
              .setEpochStartTime(epochStartTime)
              .build();

      trainer.onEpochFinished(epochData, epochInfo);

      // TODO #830: Clock should be a unit of iteration(mini-batch) instead of epoch
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
   * Called when the Task is requested to close.
   */
  void close() {
    abortFlag.set(true);
  }
}
