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
package edu.snu.cay.services.ps.worker.partitioned;

import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.task.events.TaskStop;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handlers for graceful shutdown of PartitionedParameterWorker.
 */
public class ShutdownHandlers {
  private static final Logger LOG = Logger.getLogger(ShutdownHandlers.class.getName());

  /**
   * TaskStop handler for tasks running on PS service.
   * It guarantees a task finishes after the underlying PS service completely processes queued operations.
   * With this guarantee, the context can be shutdown when there's no task running on it.
   *
   * However, we are still observing some lost messages when
   * contexts are immediately closed after the task completes.
   * It appears messages buffered in NCS are not being flushed before context close,
   * but this has to be investigated further.
   */
  public static final class TaskStopHandler implements EventHandler<TaskStop> {
    private PartitionedParameterWorker partitionedParameterWorker;

    @Inject
    private TaskStopHandler(final PartitionedParameterWorker partitionedParameterWorker) {
      this.partitionedParameterWorker = partitionedParameterWorker;
    }

    @Override
    public void onNext(final TaskStop taskStop) {
      LOG.log(Level.INFO, "Wait pending ops to be processed.");
      partitionedParameterWorker.waitPendingOps();
      LOG.log(Level.INFO, "Ops requested by the task are all processed.");
    }
  }

  /**
   * Closes the PartitionedParameterWorker.
   * Without use of {@link TaskStopHandler}, remaining operations may throw exceptions
   * on context closing, which halts the closing process.
   */
  public static final class ContextStopHandler implements EventHandler<ContextStop> {

    private PartitionedParameterWorker partitionedParameterWorker;

    @Inject
    private ContextStopHandler(final PartitionedParameterWorker partitionedParameterWorker) {
      this.partitionedParameterWorker = partitionedParameterWorker;
    }

    @Override
    public void onNext(final ContextStop contextStop) {
      partitionedParameterWorker.close();
    }
  }
}
