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

import java.util.Map;

/**
 * Provides the training data to process in mini-batches, taking subset of training data no more than
 * {@link DolphinParameters.MiniBatchSize} instances.
 * @param <K> type of the key of training data
 */
public interface TrainingDataProvider<K, V> {

  /**
   * Loads the training data.
   */
  void loadData();

  /**
   * Prepares the data to process in the next epoch, accessible with calls to {@link #getNextTrainingData()}.
   */
  void prepareDataForEpoch();

  /**
   * Provides the training data instances to compute in the next mini-batch.
   * @return a map of training data instances, which can be an empty Map if all data has been processed.
   */
  Map<K, V> getNextTrainingData();
}
