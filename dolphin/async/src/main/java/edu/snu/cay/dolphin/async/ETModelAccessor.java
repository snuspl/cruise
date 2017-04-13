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

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * An {@link ModelAccessor} implementation based on ET.
 */
public final class ETModelAccessor<K, P, V> implements ModelAccessor<K, P, V> {
  public static final String MODEL_TABLE_ID = "model_table";

  private final Table<K, V, P> modelTable;

  @Inject
  ETModelAccessor(final TableAccessor tableAccessor) throws TableNotExistException {
    this.modelTable = tableAccessor.getTable(MODEL_TABLE_ID);
  }

  @Override
  public void init(final K key, final V initValue) {
    modelTable.putIfAbsentNoReply(key, initValue);
  }

  @Override
  public void push(final K key, final P deltaValue) {
    modelTable.updateNoReply(key, deltaValue);
  }

  @Override
  public V pull(final K key) {
    final Future<V> future = modelTable.get(key);
    while (true) {
      try {
        return future.get();
      } catch (InterruptedException e) {
        // ignore and keep waiting
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public List<V> pull(final List<K> keys) {
    final List<Future<V>> resultList = new ArrayList<>(keys.size());

    keys.forEach(key -> resultList.add(modelTable.get(key)));

    final List<V> resultValues = new ArrayList<>(keys.size());
    for (final Future<V> opResult : resultList) {
      V result;
      while (true) {
        try {
          result = opResult.get();
          break;
        } catch (InterruptedException e) {
          // ignore and keep waiting
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      }

      resultValues.add(result);
    }

    return resultValues;
  }
}
