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

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.services.ps.worker.api.WorkerClock;
import org.apache.reef.driver.task.TaskConfigurationOptions.Identifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Task for a trainer thread of {@code dolphin-async} applications.
 */
@Unit
final class AsyncWorkerTask implements Task {
  private static final Logger LOG = Logger.getLogger(AsyncWorkerTask.class.getName());
  static final String TASK_ID_PREFIX = "AsyncWorkerTask";

  private final String taskId;
  private final int maxEpochs;
  private final int numMiniBatchPerEpoch;
  private final WorkerSynchronizer synchronizer;
  private final TrainingDataInitializer trainingDataInitializer;
  private final Trainer trainer;
  private final WorkerClock workerClock;
  private final TrainingDataSplitter trainingDataSplitter;
  private final MiniBatchParameterWorker miniBatchParameterWorker;

  /**
   * A boolean flag shared among all trainer threads.
   * Trainer threads end when this flag becomes true by {@link CloseEventHandler#onNext(CloseEvent)}.
   */
  private volatile boolean aborted = false;

  @Inject
  private AsyncWorkerTask(@Parameter(Identifier.class) final String taskId,
                          @Parameter(Parameters.Iterations.class) final int maxEpochs,
                          @Parameter(Parameters.MiniBatches.class) final int numMiniBatchPerEpoch,
                          final WorkerSynchronizer synchronizer,
                          final TrainingDataInitializer trainingDataInitializer,
                          final Trainer trainer,
                          final WorkerClock workerClock,
                          final TrainingDataSplitter trainingDataSplitter,
                          final MiniBatchParameterWorker miniBatchParameterWorker) {
    this.taskId = taskId;
    this.maxEpochs = maxEpochs;
    this.numMiniBatchPerEpoch = numMiniBatchPerEpoch;
    this.synchronizer = synchronizer;
    this.trainingDataInitializer = trainingDataInitializer;
    this.trainer = trainer;
    this.workerClock = workerClock;
    this.trainingDataSplitter = trainingDataSplitter;
    this.miniBatchParameterWorker = miniBatchParameterWorker;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "{0} starting...", taskId);

    trainingDataInitializer.initialize();

    // TODO #681: Need to add numWorkerThreads concept after multi-thread trainer is enabled
    trainer.initialize();

    // initialize the worker clock
    workerClock.initialize();

    // synchronize all workers before starting the main iteration
    // to avoid meaningless iterations by the workers who started earlier
    synchronizer.globalBarrier();

    // By starting training from the initial clock, which is dynamically fetched from driver,
    // it prevents workers added by EM from starting from epoch 0 and deferring job completion.
    // More specifically, added workers start from the minimum epoch of other existing workers.
    int currentEpoch = workerClock.getWorkerClock();
    final int maxIterationCount = maxEpochs * numMiniBatchPerEpoch;
    final int initialIteration = currentEpoch * numMiniBatchPerEpoch;

    for (int iteration = initialIteration; iteration < maxIterationCount; ++iteration) {
      if (aborted) {
        LOG.log(Level.INFO, "Abort a thread to completely close the task");
        // record total network waiting time of worker clock when the task is aborted
        workerClock.recordClockNetworkWaitingTime();
        return null;
      }

      final boolean isEpochStart = iteration % numMiniBatchPerEpoch == 0;
      if (isEpochStart) {
        trainingDataSplitter.prepareSplitsForEpoch();
        trainer.initEpochVariables(currentEpoch);
      }

      trainer.run();

      // flush local parameter updates to the parameter server
      miniBatchParameterWorker.flushLocalUpdates();

      final boolean isEpochEnd = (iteration + 1) % numMiniBatchPerEpoch == 0;
      if (isEpochEnd) {
        trainer.wrapUpEpochVariables(currentEpoch);
        // TODO #830: Clock should be a unit of iteration(mini-batch) instead of epoch.
        workerClock.clock();
        currentEpoch++;
      }
    }

    // Synchronize all workers before cleanup for workers
    // to finish with the globally equivalent view of trained model
    synchronizer.globalBarrier();

    trainer.cleanup();
    // record total network waiting time of worker clock when the task is finished
    workerClock.recordClockNetworkWaitingTime();
    return null;
  }

  final class CloseEventHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      aborted = true;
    }
  }
}
