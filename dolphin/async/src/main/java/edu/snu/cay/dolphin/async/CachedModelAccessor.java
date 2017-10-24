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
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 * @param <K>
 * @param <V>
 */
public final class CachedModelAccessor<K, P, V> implements ModelAccessor<K, P, V> {

  private final Map<K, V> cache = new ConcurrentHashMap<>();

  private final Table<K, V, P> modelTable;

  private final Tracer pushTracer = new Tracer();
  private final Tracer pullTracer = new Tracer();

  @Inject
  private CachedModelAccessor(@Parameter(DolphinParameters.ModelTableId.class) final String modelTableId,
                              final TableAccessor tableAccessor) throws TableNotExistException {
    this.modelTable = tableAccessor.getTable(modelTableId);
    Executors.newSingleThreadScheduledExecutor()
        .scheduleWithFixedDelay(this::refreshCache, 30, 10, TimeUnit.SECONDS);
  }

  @Override
  public void push(final K key, final P deltaValue) {
    // TODO #00: update cache

    pushTracer.startTimer();
    modelTable.updateNoReply(key, deltaValue);
    pushTracer.recordTime(1);
  }

  @Override
  public V pull(final K key) {
    // 1. in cache
    final V cachedValue = cache.get(key);
    if (cachedValue != null) {
      return cachedValue;
    } else {
      // 2. not in cache
      final V pulledValue;
      try {
        pullTracer.startTimer();
        pulledValue = modelTable.getOrInit(key).get();
        pullTracer.recordTime(1);
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      cache.put(key, pulledValue);
      return pulledValue;
    }
  }

  @Override
  public List<V> pull(final List<K> keys) {
    // 1. all values are in cache
    if (cache.keySet().containsAll(keys)) {
      final List<V> resultValues = new ArrayList<>(keys.size());
      keys.forEach(key -> resultValues.add(cache.get(key)));
      return resultValues;
    } else {
      // 2. some values are not in cache
      final Map<K, V> resultMap = new HashMap<>(keys.size());
      final Map<K, Future<V>> pullFutures = new HashMap<>();
      for (final K key : keys) {
        final V value = cache.get(key);
        if (value == null) {
          pullFutures.put(key, modelTable.getOrInit(key));
        } else {
          resultMap.put(key, value);
        }
      }

      if (!pullFutures.isEmpty()) {
        pullTracer.startTimer();
        // pull non-cached values
        pullFutures.forEach((key, valueFuture) -> {
          final V value;
          try {
            value = valueFuture.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
          cache.put(key, value);
          resultMap.put(key, value);
        });
        pullTracer.recordTime(pullFutures.size());
      }

      return new ArrayList<>(resultMap.values());
    }
  }

  @Override
  public List<V> pull(final List<K> keys, final Table aModelTable) {
    final List<Future<V>> resultList = new ArrayList<>(keys.size());
    keys.forEach(key -> resultList.add(aModelTable.getOrInit(key)));

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

  private void refreshCache() {
    final Set<K> keys = cache.keySet();
    final Map<K, Future<V>> pullFutures = new HashMap<>(keys.size());
    keys.forEach(key -> pullFutures.put(key, modelTable.getOrInit(key)));

    pullTracer.startTimer();
    pullFutures.forEach((key, pullFuture) -> {
      try {
        cache.put(key, pullFuture.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    pullTracer.recordTime(pullFutures.size());
  }
}
