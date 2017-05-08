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

import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.exceptions.TableNotExistException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * An implementation of {@link ModelAccessor} that handles push/pull requests using Elastic Tables (ET).
 * This component is responsible for collecting metrics, and is not thread-safe because the tracing components are not
 * thread-safe at the moment.
 */
@NotThreadSafe
public final class ETModelAccessor<K, P, V> implements ModelAccessor<K, P, V> {
  public static final String MODEL_TABLE_ID = "model_table";

  private final Table<K, V, P> modelTable;

  private final Tracer pushTracer = new Tracer();
  private final Tracer pullTracer = new Tracer();

  @Inject
  ETModelAccessor(final TableAccessor tableAccessor) throws TableNotExistException {
    this.modelTable = tableAccessor.getTable(MODEL_TABLE_ID);
  }

  @Override
  public void push(final K key, final P deltaValue) {
    pushTracer.startTimer();
    modelTable.updateNoReply(key, deltaValue);
    pushTracer.recordTime(1);
  }

  @Override
  public V pull(final K key) {
    pullTracer.startTimer();

    final Future<V> future = modelTable.getOrInit(key);
    V result;
    while (true) {
      try {
        result = future.get();
        break;
      } catch (InterruptedException e) {
        // ignore and keep waiting
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
    
    pullTracer.recordTime(1);
    return result;
  }

  @Override
  public List<V> pull(final List<K> keys) {
    pullTracer.startTimer();

    final List<Future<V>> resultList = new ArrayList<>(keys.size());
    keys.forEach(key -> resultList.add(modelTable.getOrInit(key)));

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

    pullTracer.recordTime(keys.size());
    return resultValues;
  }

  @Override
  public Map<String, Double> getAndResetMetrics() {
    final Map<String, Double> metrics = new HashMap<>();
    metrics.put(METRIC_TOTAL_PULL_TIME_SEC, pullTracer.totalElapsedTime());
    metrics.put(METRIC_TOTAL_PUSH_TIME_SEC, pushTracer.totalElapsedTime());
    metrics.put(METRIC_AVG_PULL_TIME_SEC, pullTracer.avgTimePerElem());
    metrics.put(METRIC_AVG_PUSH_TIME_SEC, pushTracer.avgTimePerElem());

    pullTracer.resetTrace();
    pushTracer.resetTrace();
    return metrics;
  }
}
