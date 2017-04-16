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

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import org.apache.reef.driver.task.RunningTask;

import java.util.concurrent.ExecutionException;

/**
 * Represents a submitted task.
 */
public final class SubmittedTask {
  private final RunningTask runningTask;
  private final ListenableFuture<TaskResult> resultFuture;

  SubmittedTask(final RunningTask runningTask,
                final ListenableFuture<TaskResult> resultFuture) {
    this.runningTask = runningTask;
    this.resultFuture = resultFuture;
  }

  /**
   * Stops the running task.
   */
  public void stop() {
    runningTask.close();
  }

  /**
   * @return the result of this task, after waiting it to complete
   * @throws InterruptedException when interrupted while waiting
   */
  public TaskResult getTaskResult() throws InterruptedException {
    try {
      return resultFuture.get();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final SubmittedTask that = (SubmittedTask) o;

    return (runningTask != null ? runningTask.equals(that.runningTask) : that.runningTask == null)
        && (resultFuture != null ? resultFuture.equals(that.resultFuture) : that.resultFuture == null);
  }

  @Override
  public int hashCode() {
    int result = runningTask != null ? runningTask.hashCode() : 0;
    result = 31 * result + (resultFuture != null ? resultFuture.hashCode() : 0);
    return result;
  }
}
