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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.common.impl.CallbackRegistry;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides {@link EventHandler} implementation for {@link FailedTask}.
 */
@Private
@DriverSide
public final class TaskFailedHandler implements EventHandler<FailedTask> {
  private static final Logger LOG = Logger.getLogger(TaskFailedHandler.class.getName());
  private final CallbackRegistry callbackRegistry;

  @Inject
  private TaskFailedHandler(final CallbackRegistry callbackRegistry) {
    this.callbackRegistry = callbackRegistry;
  }

  @Override
  public void onNext(final FailedTask failedTask) {
    final String taskId = failedTask.getId();
    final Optional<ActiveContext> activeContextOptional = failedTask.getActiveContext();
    if (activeContextOptional.isPresent()) {
      final String executorId = activeContextOptional.get().getEvaluatorId();
      LOG.log(Level.INFO, "Task {0} failed in executor {1}", new Object[]{taskId, executorId});
    } else {
      LOG.log(Level.INFO, "Task {0} failed", new Object[]{taskId});
    }

    callbackRegistry.onCompleted(TaskResult.class, taskId, new TaskResult(failedTask));
  }
}
