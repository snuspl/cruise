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

import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;

import java.util.Optional;

/**
 * Represents the result of a finished Task.
 */
public final class TaskResult {

  private final CompletedTask completedTask;
  private final FailedTask failedTask;

  TaskResult(final CompletedTask completedTask) {
    this.completedTask = completedTask;
    this.failedTask = null;
  }

  TaskResult(final FailedTask failedTask) {
    this.failedTask = failedTask;
    this.completedTask = null;
  }

  /**
   * @return True if the task is finished successfully
   */
  public synchronized boolean isSuccess() {
    return completedTask != null && failedTask == null;
  }

  /**
   * @return an Optional with {@link CompletedTask}
   */
  public synchronized Optional<CompletedTask> getCompletedTask() {
    return Optional.ofNullable(completedTask);
  }

  /**
   * @return an Optional with {@link FailedTask}
   */
  public synchronized Optional<FailedTask> getFailedTask() {
    return Optional.ofNullable(failedTask);
  }
}
