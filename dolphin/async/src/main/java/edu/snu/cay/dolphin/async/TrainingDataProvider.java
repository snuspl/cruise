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
import edu.snu.cay.services.em.evaluator.api.BlockUpdateListener;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides the training data to process in mini-batches, taking subset of training data no more than
 * {@link Parameters.MiniBatchSize} instances.
 * @param <K> type of the key, which should be the same with the one in MemoryStore.
 */
@TaskSide
@ThreadSafe
public final class TrainingDataProvider<K> {
  private static final Logger LOG = Logger.getLogger(TrainingDataProvider.class.getName());

  private final Set<K> trainingDataKeySet = new HashSet<>();
  private final int miniBatchSize;
  private final MemoryStore<K> memoryStore;

  /**
   * A listener registered to the MemoryStore to catch block addition/removal events.
   * the block changes in the MemoryStore will be immediately applied to training data in this provider.
   */
  private final class BlockUpdateListenerImpl implements BlockUpdateListener<K> {

    @Override
    public void onAddedBlock(final int blockId, final Set<K> addedKeys) {
      synchronized (TrainingDataProvider.this) {
        trainingDataKeySet.addAll(addedKeys);
      }
      LOG.log(Level.INFO, "Added key set size = " + addedKeys.size()
          + ", changed training data key set size = " + trainingDataKeySet.size());
    }

    @Override
    public void onRemovedBlock(final int blockId, final Set<K> removedKeys) {
      synchronized (TrainingDataProvider.this) {
        trainingDataKeySet.removeAll(removedKeys);
      }
      LOG.log(Level.INFO, "Removed key set size = " + removedKeys.size()
          + ", changed training data key set size = " + trainingDataKeySet.size());
    }
  }

  @Inject
  private TrainingDataProvider(@Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize,
                               final MemoryStore<K> memoryStore) {
    this.miniBatchSize = miniBatchSize;
    this.memoryStore = memoryStore;
    memoryStore.registerBlockUpdateListener(new BlockUpdateListenerImpl());
  }

  /**
   * Prepares the data to process in the next epoch, accessible with calls to {@link #getNextTrainingData()}.
   */
  synchronized void prepareDataForEpoch() {
    trainingDataKeySet.addAll(memoryStore.getAll().keySet());
    LOG.log(Level.INFO, "training data key set size = " + trainingDataKeySet.size());
  }

  /**
   * Provides the training data instances to compute in the next mini-batch.
   * @param <V> the type of training data
   * @return a map of training data instances, which can be an empty Map if all data has been processed.
   */
  public synchronized <V> Map<K, V> getNextTrainingData() {
    if (trainingDataKeySet.isEmpty()) {
      return Collections.emptyMap();
    }

    final Iterator<K> iterator = trainingDataKeySet.iterator();
    final List<K> nextTrainingDataKeyList = new ArrayList<>();
    while (iterator.hasNext() && nextTrainingDataKeyList.size() < miniBatchSize) {
      nextTrainingDataKeyList.add(iterator.next());
    }
    trainingDataKeySet.removeAll(nextTrainingDataKeyList);

    final Map<K, V> nextTrainingData = new HashMap<>();
    for (final K key : nextTrainingDataKeyList) {
      final Pair<K, V> keyValuePair = memoryStore.get(key);
      if (keyValuePair == null) {
        continue;
      }
      nextTrainingData.put(keyValuePair.getFirst(), keyValuePair.getSecond());
    }
    LOG.log(Level.INFO, "next training data size = " + nextTrainingData.size());
    return nextTrainingData;
  }
}
