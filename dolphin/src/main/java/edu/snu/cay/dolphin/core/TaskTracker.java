/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.core;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Dolphin task tracker on driver side.
 * REEF event handlers will use this class to memo {@code RunningTask} objects.
 * Does not guarantee safeness of using {@code getRunningTask} when dolphin job is running.
 */
public final class TaskTracker {
  private static final Logger LOG = Logger.getLogger(TaskTracker.class.getName());

  /**
   * Map of ActiveContext id to RunningTask, used to memo {@code RunningTask} object.
   */
  private final ConcurrentMap<String, RunningTask> runningTaskMap;

  @Inject
  private TaskTracker() {
    runningTaskMap = new ConcurrentHashMap<>();
  }

  public void onRunningTask(final RunningTask runningTask) {
    if (runningTaskMap.putIfAbsent(runningTask.getActiveContext().getId(), runningTask) != null) {
      LOG.log(Level.WARNING, "Failed to register running task {0} for active context {1}. Already exists.",
          new Object[]{runningTask, runningTask.getActiveContext()});
    }
  }

  public void onCompletedTask(final CompletedTask completedTask) {
    runningTaskMap.remove(completedTask.getActiveContext().getId());
  }

  public void onFailedTask(final FailedTask failedTask) {
    final Optional<ActiveContext> activeContext = failedTask.getActiveContext();
    if (activeContext.isPresent()) {
      runningTaskMap.remove(activeContext.get().getId());
    }
  }

  public void onFailedContext(final FailedContext failedContext) {
    runningTaskMap.remove(failedContext.getId());
  }

  /**
   * Retrieves memoed RunningTask. It is not safe to use this method when ControllerTask is not paused.
   * @param activeContextId
   * @return RunningTask object corresponding to the activeContextId
   */
  public RunningTask getRunningTask(final String activeContextId) {
    return runningTaskMap.get(activeContextId);
  }
}
