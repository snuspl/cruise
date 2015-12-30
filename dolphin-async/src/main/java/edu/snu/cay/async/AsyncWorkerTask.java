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
package edu.snu.cay.async;

import org.apache.reef.driver.task.TaskConfigurationOptions.Identifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Task for worker threads of {@code dolphin-async} applications.
 */
@Unit
final class AsyncWorkerTask implements Task {
  private static final Logger LOG = Logger.getLogger(AsyncWorkerTask.class.getName());
  static final String TASK_ID_PREFIX = "AsyncWorkerTask";

  private final String taskId;
  private final int maxIterations;
  private final Worker worker;
  private final CountDownLatch terminated = new CountDownLatch(1);
  private final AtomicBoolean isClosing = new AtomicBoolean(false);

  @Inject
  private AsyncWorkerTask(@Parameter(Identifier.class) final String taskId,
                         @Parameter(AsyncParameters.Iterations.class) final int maxIterations,
                         final Worker worker) {
    this.taskId = taskId;
    this.maxIterations = maxIterations;
    this.worker = worker;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "{0} starting...", taskId);

    // We do not run the worker code on this same thread,
    // to react to CloseEvents sent from the Driver (runningTask.close()).
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<?> result = executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          worker.initialize();
          for (int iteration = 0; iteration < maxIterations; ++iteration) {
            worker.run();
          }
          worker.cleanup();

        } finally {
          terminated.countDown();
        }
      }
    });

    // wait until worker thread has terminated, or Driver has sent a CloseEvent
    terminated.await();
    executor.shutdownNow();

    // check if worker thread finished cleanly or not
    try {
      result.get();
    } catch (final ExecutionException e) {
      if (isClosing.get()) {
        LOG.log(Level.INFO, "Task closed by TaskCloseHandler.");
      } else {
        throw new RuntimeException("Worker threw an exception", e);
      }
    }

    return null;
  }

  /**
   * Handler for {@link CloseEvent} sent from Driver.
   * Stop this running task without exceptions.
   */
  final class TaskCloseHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      if (terminated.getCount() == 0) {
        LOG.log(Level.INFO, "Tried to close task, but worker thread already terminated.");

      } else {
        LOG.log(Level.INFO, "Task closing {0}", closeEvent);
        isClosing.compareAndSet(false, true);
        terminated.countDown();
      }
    }
  }
}
