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

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Split training data in memory store and provide a chunk of data.
 * The chunk size is total data size / numMiniBatchesPerEpoch.
 * @param <K> type of the key (or id), it should be the same type with keys of MemoryStore.
 */
@TaskSide
@NotThreadSafe
public final class TrainingDataSplitter<K> {
  private final int numMiniBatchesPerEpoch;

  private final MemoryStore<K> memoryStore;

  private final List<List<K>> trainingDataSplits;

  private int currentMiniBatch;

  @Inject
  private TrainingDataSplitter(@Parameter(Parameters.MiniBatches.class) final int numMiniBatchPerEpoch,
                              final MemoryStore<K> memoryStore) {
    this.numMiniBatchesPerEpoch = numMiniBatchPerEpoch;
    this.memoryStore = memoryStore;
    this.trainingDataSplits = new ArrayList<>();
    this.currentMiniBatch = 0;
  }

  /**
   * Split training data and reset variables.
   */
  public void prepareSplitsForEpoch() {
    trainingDataSplits.clear();

    final Set<K> keys = memoryStore.getAll().keySet();
    // TODO #824: Fix the number of instances to be processed in a mini-batch.
    final int sizeOfTrainingDataSplit = keys.size() / numMiniBatchesPerEpoch;

    int splitIndex = 0;
    for (final K key : keys) {
      if (trainingDataSplits.size() < splitIndex + 1) {
        trainingDataSplits.add(new ArrayList());
      }
      trainingDataSplits.get(splitIndex).add(key);
      // last split should be bigger than sizeOfTrainingDataSplit if keys.size() % numMiniBatchesPerEpoch != 0
      if (trainingDataSplits.get(splitIndex).size() == sizeOfTrainingDataSplit
          && (splitIndex + 1) < numMiniBatchesPerEpoch) {
        splitIndex++;
      }
    }

    currentMiniBatch = 0;
  }

  /**
   * Provide next training data split to compute in the current mini-batch.
   * @param <V> the type of training data
   * @return map of training data split of the current mini-batch,
   *         otherwise return empty map
   */
  public <V> Map<K, V> getNextTrainingDataSplit() {
    if (trainingDataSplits.size() < (currentMiniBatch + 1)) {
      return Collections.emptyMap();
    }

    final List<K> keyList = trainingDataSplits.get(currentMiniBatch++);
    final Map<K, V> nextTrainingDataSplit = new HashMap<>();
    for (final K key : keyList) {
      final Pair<K, V> keyValuePair = memoryStore.get(key);
      if (keyValuePair == null) {
        continue;
      }
      nextTrainingDataSplit.put(keyValuePair.getFirst(), keyValuePair.getSecond());
    }
    return nextTrainingDataSplit;
  }
}
