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
package edu.snu.cay.dolphin.async;

import org.apache.reef.annotations.audience.TaskSide;

/**
 * A single worker thread of a {@code dolphin-async} application.
 *
 * Classes implementing this interface should consist of
 * a main {@code run} method that is executed once every iteration, as well as
 * a pre-run method {@code initialize} and post-run method {@code cleanup}, which are
 * executed before and after the main run loop, respectively.
 *
 * Implementations should also have at least one constructor that is marked with the {@link javax.inject.Inject}
 * annotation, so that the framework can successfully instantiate the class via dependency injection.
 *
 * To interact with the parameter server, declare a {@link edu.snu.cay.services.ps.worker.api.ParameterWorker} as
 * a constructor parameter of an {@link javax.inject.Inject}-annotated constructor, and store it in a field for later
 * use at {@code run} or the other methods.
 *
 * Other parameters specified with {@link AsyncDolphinConfiguration.Builder#addParameterClass(Class)}
 * can also be received as constructor parameters, given that the parameter itself is tagged with
 * {@link org.apache.reef.tang.annotations.Parameter} and an actual value is given for the parameter via command line.
 */
@TaskSide
public interface Worker {

  /**
   * Pre-run method that is executed after the constructor but before {@code run}, exactly once.
   */
  void initialize();

  /**
   * Main method of this worker. The number of times this method is called can be adjusted with the parameter
   * {@link edu.snu.cay.common.param.Parameters.Iterations}.
   */
  void run();

  /**
   * Post-run method executed after {@code run} but before worker termination, exactly once.
   */
  void cleanup();
}
