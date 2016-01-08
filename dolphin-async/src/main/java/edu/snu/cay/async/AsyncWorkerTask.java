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

import edu.snu.cay.common.param.Parameters;
import org.apache.reef.driver.task.TaskConfigurationOptions.Identifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Task for worker threads of {@code dolphin-async} applications.
 */
final class AsyncWorkerTask implements Task {
  private static final Logger LOG = Logger.getLogger(AsyncWorkerTask.class.getName());
  static final String TASK_ID_PREFIX = "AsyncWorkerTask";

  private final String taskId;
  private final int maxIterations;
  private final Worker worker;

  @Inject
  private AsyncWorkerTask(@Parameter(Identifier.class) final String taskId,
                          @Parameter(Parameters.Iterations.class) final int maxIterations,
                          final Worker worker) {
    this.taskId = taskId;
    this.maxIterations = maxIterations;
    this.worker = worker;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "{0} starting...", taskId);

    worker.initialize();
    for (int iteration = 0; iteration < maxIterations; ++iteration) {
      worker.run();
    }
    worker.cleanup();

    return null;
  }
}
