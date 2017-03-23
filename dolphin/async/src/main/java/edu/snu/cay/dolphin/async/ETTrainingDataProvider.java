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
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides the training data to process in mini-batches, taking subset of training data no more than
 * {@link DolphinParameters.MiniBatchSize} instances.
 * This class is designed to handle Trainer threads' concurrent accesses to the training data.
 * @param <K> type of the key, which should be the same with the one in MemoryStore.
 */
@TaskSide
@ThreadSafe
public final class ETTrainingDataProvider<K, V> implements TrainingDataProvider {
  private static final Logger LOG = Logger.getLogger(ETTrainingDataProvider.class.getName());
  static final String TRAINING_DATA_TABLE_ID = "training_data_table";

  private final List<K> trainingDataKeys = Collections.synchronizedList(new LinkedList<>());

  private final int miniBatchSize;

  private final Table<K, V> trainingDataTable;

  @Inject
  private ETTrainingDataProvider(@Parameter(DolphinParameters.MiniBatchSize.class) final int miniBatchSize,
                               final TableAccessor tableAccessor) throws TableNotExistException {
    this.trainingDataTable = tableAccessor.get(TRAINING_DATA_TABLE_ID);
    this.miniBatchSize = miniBatchSize;
  }

  @Override
  public void loadData() {
    // data are already loaded into table when it was initialized
  }

  @Override
  public void prepareDataForEpoch() {
    synchronized (trainingDataKeys) {
      trainingDataKeys.addAll(trainingDataTable.getLocalDataMap().keySet());
      Collections.shuffle(trainingDataKeys);
      LOG.log(Level.INFO, "training data key set size = {0}", trainingDataKeys.size());
    }
  }

  /**
   * Provides the training data instances to compute in the next mini-batch.
   * @return a map of training data instances, which can be an empty Map if all data has been processed.
   */
  @Override
  public Map<K, V> getNextTrainingData() {
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
      final V value = trainingDataTable.get(key);
      if (value == null) {
        continue;
      }
      nextTrainingData.put(key, value);
    }

    LOG.log(Level.INFO, "Size of training data for next mini-batch: {0}", nextTrainingData.size());
    if (nextTrainingDataKeyList.size() != nextTrainingData.size()) {
      LOG.log(Level.INFO, "The number of assigned data keys for next mini-batch is {0}," +
              " but ths size of actually prepared training data is {1}",
          new Object[]{nextTrainingDataKeyList.size(), nextTrainingData.size()});
    }

    return nextTrainingData;
  }
}
