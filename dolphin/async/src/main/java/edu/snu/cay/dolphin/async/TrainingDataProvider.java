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

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

/**
 * Provides the training data to process during epoch, for mini-batches.
 * @param <K> type of the key of training data
 */
public interface TrainingDataProvider<K, V> {

  /**
   * Prepares the data to process in the next epoch, accessible with calls to {@link #getNextBatchData()}.
   */
  void prepareDataForEpoch();

  /**
   * Provides the training data instances to compute in the next mini-batch.
   * @return a list of key-value data pairs, which can be an empty list if all data has been processed.
   */
  List<Pair<K, V>> getNextBatchData();

  /**
   * Provides the training data for this epoch.
   * @return a map of training data instances for epoch
   */
  Map<K, V> getEpochData();
}
