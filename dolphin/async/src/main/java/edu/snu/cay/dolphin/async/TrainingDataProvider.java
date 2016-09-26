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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Provides the training data to process in mini-batches, taking subset of training data no more than
 * {@link Parameters.MiniBatchSize} instances.
 * @param <K> type of the key, which should be the same with the one in MemoryStore.
 */
@TaskSide
@NotThreadSafe
public final class TrainingDataProvider<K> {
  private final int miniBatchSize;

  private final MemoryStore<K> memoryStore;

  /**
   * An iterator for the training data; each element is a list of data keys.
   */
  private Iterator<List<K>> trainingDataKeysIterator;

  @Inject
  private TrainingDataProvider(@Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize,
                               final MemoryStore<K> memoryStore) {
    this.miniBatchSize = miniBatchSize;
    this.memoryStore = memoryStore;
    this.trainingDataKeysIterator = Collections.emptyIterator();
  }

  /**
   * Prepares the data to process in the next epoch, accessible with calls to {@link #getNextTrainingData()}.
   */
  void prepareDataForEpoch() {
    final List<K> keys = new ArrayList<>(memoryStore.getAll().keySet());
    final int numMiniBatches = (int) Math.ceil((double) keys.size() / miniBatchSize);
    final List<List<K>> keysList = new ArrayList<>(numMiniBatches);

    final int remainderForLastMiniBatch = keys.size() % miniBatchSize;
    final int numInstancesForLastMiniBatch = remainderForLastMiniBatch == 0 ? miniBatchSize : remainderForLastMiniBatch;

    int numKeysCounted = 0;
    int numKeysToCount = miniBatchSize;

    for (int miniBatchIdx = 0; miniBatchIdx < numMiniBatches; miniBatchIdx++) {
      if (miniBatchIdx == numMiniBatches - 1) {
        numKeysToCount = numInstancesForLastMiniBatch;
      }

      final List<K> trainingData = keys.subList(numKeysCounted, numKeysCounted + numKeysToCount);
      numKeysCounted += numKeysToCount;

      keysList.add(trainingData);
    }

    trainingDataKeysIterator = keysList.iterator();
  }

  /**
   * Provides the training data instances to compute in the next mini-batch.
   * @param <V> the type of training data
   * @return a map of training data instances, which can be an empty Map if all data has been processed.
   */
  public <V> Map<K, V> getNextTrainingData() {
    if (!trainingDataKeysIterator.hasNext()) {
      return Collections.emptyMap();
    }

    final List<K> keyList = trainingDataKeysIterator.next();
    final Map<K, V> nextTrainingData = new HashMap<>();
    for (final K key : keyList) {
      // TODO #464: Add getList() API to MemoryStore
      final Pair<K, V> keyValuePair = memoryStore.get(key);
      if (keyValuePair == null) {
        continue;
      }
      nextTrainingData.put(keyValuePair.getFirst(), keyValuePair.getSecond());
    }
    return nextTrainingData;
  }
}
