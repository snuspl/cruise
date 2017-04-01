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

import edu.snu.cay.services.et.evaluator.api.DataOpResult;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.exceptions.TableNotExistException;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * An {@link ModelAccessor} implementation based on ET.
 */
public final class ETModelAccessor<K, P, V> implements ModelAccessor<K, P, V> {
  static final String MODEL_TABLE_ID = "model_table";

  private final Table<K, V, P> modelTable;

  @Inject
  ETModelAccessor(final TableAccessor tableAccessor) throws TableNotExistException {
    this.modelTable = tableAccessor.get(MODEL_TABLE_ID);
  }

  @Override
  public void init(final K key, final V initValue) {
    modelTable.putIfAbsentAsync(key, initValue);
  }

  @Override
  public void push(final K key, final P deltaValue) {
    modelTable.updateAsync(key, deltaValue);
  }

  @Override
  public V pull(final K key) {
    return modelTable.get(key);
  }

  @Override
  public List<V> pull(final List<K> keys) {
    final List<DataOpResult<V>> resultList = new ArrayList<>(keys.size());

    keys.forEach(key -> resultList.add(modelTable.getAsync(key)));

    final List<V> resultValues = new ArrayList<>(keys.size());
    for (final DataOpResult<V> opResult : resultList) {
      boolean completed = false;
      while (!completed) {
        try {
          opResult.waitRemoteOp();
          completed = true;
        } catch (InterruptedException e) {
          // ignore interrupt
        }
      }

      resultValues.add(opResult.getOutputData());
    }

    return resultValues;
  }
}
