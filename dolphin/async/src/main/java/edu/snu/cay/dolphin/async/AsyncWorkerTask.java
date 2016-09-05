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
  private final MemoryStoreInitializer memoryStoreInitializer;
  private final Trainer trainer;
  private final WorkerClock workerClock;
  private final TrainingDataDivider trainingDataDivider;
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
                          final MemoryStoreInitializer memoryStoreInitializer,
                          final Trainer trainer,
                          final WorkerClock workerClock,
                          final TrainingDataDivider trainingDataDivider,
                          final MiniBatchParameterWorker miniBatchParameterWorker) {
    this.taskId = taskId;
    this.maxEpochs = maxEpochs;
    this.numMiniBatchPerEpoch = numMiniBatchPerEpoch;
    this.synchronizer = synchronizer;
    this.memoryStoreInitializer = memoryStoreInitializer;
    this.trainer = trainer;
    this.workerClock = workerClock;
    this.trainingDataDivider = trainingDataDivider;
    this.miniBatchParameterWorker = miniBatchParameterWorker;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "{0} starting...", taskId);

    memoryStoreInitializer.initialize();

    // TODO #681: Need to add numWorkerThreads concept after multi-thread trainer is enabled
    trainer.initialize();

    // synchronize all workers before starting the main iteration
    // to avoid meaningless iterations by the workers who started earlier
    synchronizer.globalBarrier();

    // initialize the worker clock
    workerClock.initialize();

    // By starting iteration from the initial clock, which is dynamically fetched from driver,
    // it prevents workers added by EM from starting from iteration 0 and deferring job completion.
    // More specifically, added workers start from the minimum iteration of other existing workers.
    for (int epoch = 0; epoch < maxEpochs; ++epoch) {
      if (aborted) {
        LOG.log(Level.INFO, "Abort a thread to completely close the task");
        // record total network waiting time of worker clock when the task is aborted
        workerClock.recordClockNetworkWaitingTime();
        return null;
      }

      if (workerClock.getWorkerClock() > maxEpochs * numMiniBatchPerEpoch) {
        break;
      }

      trainingDataDivider.onEpochStart();
      trainer.onEpochStart(epoch);

      for (int minibatch = 0; minibatch < numMiniBatchPerEpoch; minibatch++) {
        if (workerClock.getWorkerClock() > maxEpochs * numMiniBatchPerEpoch) {
          break;
        }

        trainer.run(minibatch);
        miniBatchParameterWorker.onMiniBatchEnd();
        trainingDataDivider.onMiniBatchEnd();
        workerClock.clock();
      }

      trainer.onEpochEnd(epoch);
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
