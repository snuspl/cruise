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
package edu.snu.cay.dolphin.async;

import org.apache.reef.annotations.audience.TaskSide;

import java.util.Collection;

/**
 * A trainer of a {@code dolphin-async} application.
 *
 * Classes implementing this interface should consist of
 * a main {@code run} method that is executed once every iteration, as well as
 * a pre-run method {@code initGlobalSettings} and post-run method {@code cleanup}, which are
 * executed before and after the main run loop, respectively. Note that {@code initGlobalSettings} is optional, since
 * we do not need to initialize global settings while the job is running already.
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
 *
 * @param <D> type of the training data
 */
@TaskSide
public interface Trainer<D> {

  /**
   * Pre-run method that initializes the global settings (e.g., model parameters).
   * This method is executed exactly once before {@code run}, if the job's state is
   * in {@link SynchronizationManager.State#INIT}. Note that this method is skipped in other states.
   */
  void initGlobalSettings();

  /**
   * Main method of this trainer, which is called every mini-batch.
   * @param miniBatchData the training data to process in the batch
   *                      (at most {@link edu.snu.cay.dolphin.async.DolphinParameters.MiniBatchSize} instances.
   * @param testData the test data to evaluate the model computed in the batch
   * @return a result of the mini-batch
   */
  MiniBatchResult runMiniBatch(Collection<D> miniBatchData, Collection<D> testData);

  /**
   * EventHandler that is called when an epoch is finished.
   * @param epochData the training data that has been processed in the epoch
   * @param testData the test data to evaluate the model computed in the epoch
   * @param epochIdx the index of the epoch
   * @return a result of the epoch
   */
  EpochResult onEpochFinished(Collection<D> epochData, Collection<D> testData, int epochIdx);

  /**
   * Post-run method executed after {@code run} but before task termination, exactly once.
   */
  void cleanup();
}
