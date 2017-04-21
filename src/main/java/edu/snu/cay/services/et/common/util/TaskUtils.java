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
package edu.snu.cay.services.et.common.util;

import edu.snu.cay.services.et.driver.impl.SubmittedTask;
import edu.snu.cay.services.et.driver.impl.TaskResult;
import org.apache.reef.tang.Configuration;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Provides utility methods for managing tasks.
 */
public final class TaskUtils {
  /**
   * Utility classes should not be instantiated.
   */
  private TaskUtils() {
  }

  /**
   * Wait until all Tasks finish and checks whether the results are as expected.
   * @param taskFutureList The list of futures of submitted tasks, each of which is returned
   *                       by {@link edu.snu.cay.services.et.driver.api.AllocatedExecutor#submitTask(Configuration)}
   * @param expected The expected answer.
   */
  public static void waitAndCheckTaskResult(final List<Future<SubmittedTask>> taskFutureList, final boolean expected) {
    taskFutureList.forEach(taskResultFuture -> {
      try {
        final TaskResult taskResult = taskResultFuture.get().getTaskResult();
        if (taskResult.isSuccess() != expected) {
          final String taskId = taskResult.getFailedTask().get().getId();
          throw new RuntimeException(String.format("Task %s has been failed", taskId));
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
