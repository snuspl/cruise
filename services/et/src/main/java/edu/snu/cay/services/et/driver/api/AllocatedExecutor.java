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
package edu.snu.cay.services.et.driver.api;

import edu.snu.cay.services.et.avro.TaskletStatusMsg;
import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.configuration.TaskletConfiguration;
import edu.snu.cay.services.et.driver.impl.RunningTasklet;
import edu.snu.cay.services.et.driver.impl.TaskletResult;
import org.apache.reef.annotations.audience.DriverSide;

import java.util.Set;

/**
 * Represents an allocated executor.
 */
@DriverSide
public interface AllocatedExecutor {
  /**
   * @return the identifier of the allocated executor.
   */
  String getId();

  /**
   * Submit tasklet to executor.
   * @param taskletConf tasklet configuration.
   * @return a {@link ListenableFuture} of {@link TaskletResult}
   */
  ListenableFuture<RunningTasklet> submitTasklet(TaskletConfiguration taskletConf);

  /**
   * It's for internal usage.
   * @param taskletId tasklet Id
   * @param taskletStatusMsg tasklet status msg from tasklets in executors
   */
  void onTaskletStatusMessage(String taskletId, TaskletStatusMsg taskletStatusMsg);

  /**
   * @return a set of running tasklet Ids
   */
  Set<String> getTaskletIds();

  /**
   * Get a {@link RunningTasklet} that has given tasklet Id.
   * @param taskletId a tasklet Id
   * @return a {@link RunningTasklet} with {@code taskletId} or {@code null} if it does not exist
   */
  RunningTasklet getRunningTasklet(String taskletId);

  /**
   * Closes the executor.
   * @return a {@link ListenableFuture} that completes upon executor close
   */
  ListenableFuture<Void> close();
}
