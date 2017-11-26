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

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.driver.impl.RunningTasklet;
import edu.snu.cay.services.et.driver.impl.TaskletResult;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Configuration;

import java.util.Map;
import java.util.Optional;

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
   * Assign task to executor.
   * @param taskConf task configuration.
   * @return a {@link ListenableFuture} of {@link TaskletResult}
   */
  ListenableFuture<RunningTasklet> submitTask(Configuration taskConf);

  /**
   * @return an {@link Optional} with a {@link RunningTasklet} submitted by {@link #submitTask(Configuration)}
   * It's emtpy when there's no running task.
   */
  Map<String, RunningTasklet> getRunningTasks();

  /**
   * Closes the executor.
   * @return a {@link ListenableFuture} that completes upon executor close
   */
  ListenableFuture<Void> close();
}
