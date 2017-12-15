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
package edu.snu.spl.cruise.services.et.common.util;

import edu.snu.spl.cruise.services.et.driver.impl.RunningTasklet;
import edu.snu.spl.cruise.services.et.driver.impl.TaskletResult;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Provides utility methods for managing tasklets.
 */
public final class TaskletUtils {
  /**
   * Utility classes should not be instantiated.
   */
  private TaskletUtils() {
  }

  /**
   * Wait until all Tasklets finish and checks whether the results are as expected.
   * @param taskletFutureList The list of futures of {@link RunningTasklet}s
   * @param expected The expected answer.
   */
  public static void waitAndCheckTaskletResult(final List<Future<RunningTasklet>> taskletFutureList,
                                               final boolean expected) {
    taskletFutureList.forEach(taskFuture -> {
      try {
        final RunningTasklet runningTasklet = taskFuture.get();
        final TaskletResult taskletResult = runningTasklet.getTaskResult();
        if (taskletResult.isSuccess() != expected) {
          final String taskletId = runningTasklet.getId();
          throw new RuntimeException(String.format("Tasklet %s has been failed", taskletId));
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
