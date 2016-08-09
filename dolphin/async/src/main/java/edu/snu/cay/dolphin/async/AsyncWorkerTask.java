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
 * REEF Task for a worker thread of {@code dolphin-async} applications.
 */
@Unit
final class AsyncWorkerTask implements Task {
  private static final Logger LOG = Logger.getLogger(AsyncWorkerTask.class.getName());
  static final String TASK_ID_PREFIX = "AsyncWorkerTask";

  private final String taskId;
  private final int maxIterations;
  private final WorkerSynchronizer synchronizer;
  private final Worker worker;
  private final WorkerClock workerClock;

  /**
   * A boolean flag shared among all worker threads.
   * Worker threads end when this flag becomes true by {@link CloseEventHandler#onNext(CloseEvent)}.
   */
  private volatile boolean aborted = false;

  @Inject
  private AsyncWorkerTask(@Parameter(Identifier.class) final String taskId,
                          @Parameter(Iterations.class) final int maxIterations,
                          final WorkerSynchronizer synchronizer,
                          final Worker worker,
                          final WorkerClock workerClock) {
    this.taskId = taskId;
    this.maxIterations = maxIterations;
    this.synchronizer = synchronizer;
    this.worker = worker;
    this.workerClock = workerClock;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "{0} starting...", taskId);

    // TODO #681: Need to add numWorkerThreads concept after multi-thread worker is enabled
    worker.initialize();

    // synchronize all workers before starting the main iteration
    // to avoid meaningless iterations by the workers who started earlier
    synchronizer.globalBarrier();

    // initialize the worker clock
    workerClock.initialize();

    for (int iteration = 0; iteration < maxIterations; ++iteration) {
      if (aborted) {
        LOG.log(Level.INFO, "Abort a thread to completely close the task");
        return null;
      }
      worker.run();
      workerClock.clock();
    }

    // Synchronize all workers before cleanup for workers
    // to finish with the globally equivalent view of trained model
    synchronizer.globalBarrier();

    worker.cleanup();
    return null;
  }

  final class CloseEventHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      // record total network waiting time of worker clock when the worker is closed
      workerClock.recordClockNetworkWaitingTime();
      aborted = true;
    }
  }
}
