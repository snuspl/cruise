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

import edu.snu.cay.services.em.evaluator.api.BlockUpdateListener;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of TrainingDataProvider based on ElasticMemory. The training data is stored in the
 * MemoryStore.
 * This class is designed to handle concurrent accesses to the training data,
 * and react to block migration by registering {@link BlockUpdateListener}.
 * @param <K> type of the key, which should be the same with the one in MemoryStore.
 */
@ThreadSafe
@TaskSide
final class EMTrainingDataProvider<K, V> implements TrainingDataProvider<K, V> {
  private static final Logger LOG = Logger.getLogger(EMTrainingDataProvider.class.getName());

  private final List<K> trainingDataKeys = Collections.synchronizedList(new LinkedList<>());

  private final int miniBatchSize;

  private final MemoryStore<K> memoryStore;
  private final DataIdFactory<K> dataIdFactory;
  private final DataParser<V> dataParser;

  @Inject
  private EMTrainingDataProvider(@Parameter(DolphinParameters.MiniBatchSize.class) final int miniBatchSize,
                                 final MemoryStore<K> memoryStore,
                                 final DataIdFactory<K> dataIdFactory,
                                 final DataParser<V> dataParser) {
    this.miniBatchSize = miniBatchSize;
    this.memoryStore = memoryStore;
    this.dataIdFactory = dataIdFactory;
    this.dataParser = dataParser;
    memoryStore.registerBlockUpdateListener(new BlockUpdateListenerImpl());
  }

  /**
   * Loads the training data into MemoryStore.
   */
  @Override
  public void loadData() {
    final List<V> dataValues = dataParser.parse();
    final List<K> dataKeys;
    try {
      dataKeys = dataIdFactory.getIds(dataValues.size());
    } catch (final IdGenerationException e) {
      throw new RuntimeException("Fail to generate data keys", e);
    }

    memoryStore.putList(dataKeys, dataValues);

    LOG.log(Level.INFO, "Total number of training data items = {0}", dataValues.size());
  }

  @Override
  public void prepareDataForEpoch() {
    synchronized (trainingDataKeys) {
      trainingDataKeys.clear();
      trainingDataKeys.addAll(memoryStore.getAll().keySet());
      Collections.shuffle(trainingDataKeys);
      LOG.log(Level.INFO, "training data key set size = {0}", trainingDataKeys.size());
    }
  }

  @Override
  public Map<K, V> getNextBatchData() {
    final List<K> nextTrainingDataKeyList;
    synchronized (trainingDataKeys) {
      if (trainingDataKeys.isEmpty()) {
        LOG.log(Level.INFO, "no more training data for current epoch");
        return Collections.emptyMap();
      }

      final int nextBatchSize = Math.min(miniBatchSize, trainingDataKeys.size());
      final List<K> keysToTrain = trainingDataKeys.subList(0, nextBatchSize);
      nextTrainingDataKeyList = new ArrayList<>(keysToTrain);
      keysToTrain.clear();
    }

    final Map<K, V> nextTrainingData = new HashMap<>();
    for (final K key : nextTrainingDataKeyList) {
      // TODO #464: Add getList() API to MemoryStore
      final Pair<K, V> keyValuePair = memoryStore.get(key);
      if (keyValuePair == null) {
        continue;
      }
      nextTrainingData.put(keyValuePair.getFirst(), keyValuePair.getSecond());
    }

    LOG.log(Level.INFO, "Size of training data for next mini-batch: {0}", nextTrainingData.size());
    if (nextTrainingDataKeyList.size() != nextTrainingData.size()) {
      LOG.log(Level.INFO, "The number of assigned data keys for next mini-batch is {0}," +
          " but ths size of actually prepared training data is {1}",
          new Object[]{nextTrainingDataKeyList.size(), nextTrainingData.size()});
    }

    return nextTrainingData;
  }

  @Override
  public Map<K, V> getEpochData() {
    return memoryStore.getAll();
  }

  /**
   * A listener registered to the MemoryStore to catch block addition/removal events.
   * the block changes in the MemoryStore will be immediately applied to training data in this provider.
   */
  private final class BlockUpdateListenerImpl implements BlockUpdateListener<K> {

    @Override
    public void onAddedBlock(final int blockId, final Set<K> addedKeys) {
      synchronized (trainingDataKeys) {
        trainingDataKeys.addAll(addedKeys);
        LOG.log(Level.INFO, "The number of added keys: {0}, the number of training data after addition: {1}",
            new Object[] {addedKeys.size(), trainingDataKeys.size()});
      }
    }

    @Override
    public void onRemovedBlock(final int blockId, final Set<K> removedKeys) {
      synchronized (trainingDataKeys) {
        trainingDataKeys.removeAll(removedKeys);
        LOG.log(Level.INFO, "The number of removed keys: {0}, the number of training data after removal: {1}",
            new Object[] {removedKeys.size(), trainingDataKeys.size()});
      }
    }
  }
}
