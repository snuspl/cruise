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

import edu.snu.cay.services.et.evaluator.api.Table;
import org.apache.reef.annotations.audience.TaskSide;

import java.util.Collection;
import java.util.Map;

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
 * Other parameters specified with {@link ETDolphinConfiguration.Builder#addParameterClass(Class)}
 * can also be received as constructor parameters, given that the parameter itself is tagged with
 * {@link org.apache.reef.tang.annotations.Parameter} and an actual value is given for the parameter via command line.
 *
 * @param <D> type of the training data
 */
@TaskSide
public interface Trainer<D> {

  /**
   * Pre-run method that initializes the global settings (e.g., model parameters).
   * This method is executed exactly once before {@code run}.
   */
  void initGlobalSettings();

  /**
   * Main method of this trainer, which is called every mini-batch.
   * @param miniBatchTrainingData the training data to process in the batch,
   *         at most (the number of training data set / {@link DolphinParameters.NumTotalMiniBatches}) instances.
   */
  void runMiniBatch(Collection<D> miniBatchTrainingData);

  /**
   * EventHandler that is called when an epoch is finished.
   * @param epochIdx the index of the epoch
   */
  void onEpochFinished(int epochIdx);

  /**
   * Evaluate a model with a given input data and test data.
   * @param inputData the data that it evaluates the model with
   * @param testData the test data to evaluate the model computed in the epoch
   * @param modelTable a model table to evaluate
   * @return the result value of evaluation
   */
  Map<CharSequence, Double> evaluateModel(Collection<D> inputData, Collection<D> testData, Table modelTable);

  /**
   * Post-run method executed after {@code run} but before task termination, exactly once.
   */
  void cleanup();
}
