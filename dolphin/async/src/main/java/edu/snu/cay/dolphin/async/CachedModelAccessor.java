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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;

/**
 * A {@link ModelAccessor} implementation with model cache.
 */
public final class CachedModelAccessor<K, P, V> implements ModelAccessor<K, P, V> {

  private final LoadingCache<K, V> modelLoadingCache;

  private final Table<K, V, P> modelTable;
  private final UpdateFunction<K, V, P> modelUpdateFunction;

  private final Tracer pushTracer = new Tracer();
  private final Tracer pullTracer = new Tracer();

  @Inject
  private CachedModelAccessor(@Parameter(DolphinParameters.ModelTableId.class) final String modelTableId,
                              final TableAccessor tableAccessor,
                              final UpdateFunction<K, V, P> modelUpdateFunction) throws TableNotExistException {
    this.modelTable = tableAccessor.getTable(modelTableId);
    this.modelUpdateFunction = modelUpdateFunction;

    this.modelLoadingCache = initCache();
  }

  private LoadingCache<K, V> initCache() {
    return CacheBuilder.newBuilder()
        .refreshAfterWrite(10, TimeUnit.SECONDS) // TODO #1254: introduce a sophisticated cache refresh/eviction policy
        .concurrencyLevel(4)
        .build(new CacheLoader<K, V>() {
          @Override
          public V load(final K key) throws Exception {
            pullTracer.startTimer();
            final Future<V> pullFuture = modelTable.getOrInit(key);
            final V value = pullFuture.get();
            pullTracer.recordTime(1);
            return value;
          }

          @Override
          public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
            final List<Future<V>> pullFutureList = new ArrayList<>();
            keys.forEach(key -> {
              pullFutureList.add(modelTable.getOrInit(key));// TODO: ET#176 support multi-get operation
            });

            final Map<K, V> resultKVMap = new HashMap<>(pullFutureList.size());

            int i = 0;
            for (final K key : keys) {
              resultKVMap.put(key, pullFutureList.get(i++).get());
            }

            return resultKVMap;
          }
        });
  }

  /**
   * Push a delta value for a key, applying the change to cache.
   */
  @Override
  public void push(final K key, final P deltaValue) {
    pushTracer.startTimer();
    modelTable.updateNoReply(key, deltaValue);
    pushTracer.recordTime(1);

    // update value in cache. this modification will not cause entry loading in cache.
    modelLoadingCache.asMap().
        computeIfPresent(key, (k, oldValue) -> modelUpdateFunction.updateValue(k, oldValue, deltaValue));
  }

  /**
   * Retrieve a value for a requested key.
   * Pull value from servers, if cache does not have value for the key.
   */
  @Override
  public V pull(final K key) {
    try {
      return modelLoadingCache.get(key);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieve values for requested keys.
   * Pull values from servers, if cache does not have all values for the keys.
   */
  @Override
  public List<V> pull(final List<K> keys) {
    try {
      final Map<K, V> kvMap = modelLoadingCache.getAll(keys);
      final List<V> valueList = new ArrayList<>(keys.size());
      keys.forEach(key -> valueList.add(kvMap.get(key)));

      return valueList;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This method does not care about cache.
   */
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
}
