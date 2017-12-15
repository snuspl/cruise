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
package edu.snu.spl.cruise.ps;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.snu.spl.cruise.ps.core.worker.ModelAccessor;
import edu.snu.spl.cruise.ps.metric.Tracer;
import edu.snu.spl.cruise.services.et.evaluator.api.Table;
import edu.snu.spl.cruise.services.et.evaluator.api.TableAccessor;
import edu.snu.spl.cruise.services.et.evaluator.api.UpdateFunction;
import edu.snu.spl.cruise.services.et.exceptions.TableNotExistException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;

/**
 * A {@link ModelAccessor} implementation with model cache.
 */
public final class CachedModelAccessor<K, P, V> implements ModelAccessor<K, P, V> {
  private static final int MODEL_REFRESH_SEC = 10; // TODO #1254: introduce a sophisticated cache policy
  private static final int CACHE_CONCURRENCY_WRITES = 4;

  private final LoadingCache<K, V> modelLoadingCache;

  private final Table<K, V, P> modelTable;
  private final UpdateFunction<K, V, P> modelUpdateFunction;

  private final ScheduledExecutorService refreshExecutor;

  private final Tracer pushTracer = new Tracer();
  private final Tracer pullTracer = new Tracer();

  @Inject
  private CachedModelAccessor(@Parameter(DolphinParameters.ModelTableId.class) final String modelTableId,
                              final TableAccessor tableAccessor,
                              final UpdateFunction<K, V, P> modelUpdateFunction) throws TableNotExistException {
    this.modelTable = tableAccessor.getTable(modelTableId);
    this.modelUpdateFunction = modelUpdateFunction;

    this.modelLoadingCache = initCache();

    refreshExecutor = Executors.newSingleThreadScheduledExecutor();
    refreshExecutor.scheduleWithFixedDelay(() -> {
      final Set<K> keys = modelLoadingCache.asMap().keySet();

      if (!keys.isEmpty()) {
        final List<K> keyList = new ArrayList<>(keys.size());
        try {
          pullTracer.startTimer();
          final Map<K, V> kvMap = modelTable.multiGetOrInit(keyList).get();
          pullTracer.recordTime(keys.size());

          kvMap.forEach(modelLoadingCache::put);
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }

    }, 0, MODEL_REFRESH_SEC, TimeUnit.SECONDS);
  }

  public void stopRefreshingCache() {
    refreshExecutor.shutdown();
  }

  private LoadingCache<K, V> initCache() {
    return CacheBuilder.newBuilder()
        .concurrencyLevel(CACHE_CONCURRENCY_WRITES)
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
          public Map<K, V> loadAll(final Iterable<? extends K> keys) throws Exception {
            final List<K> keyList = new ArrayList<>();
            keys.forEach(keyList::add);

            pullTracer.startTimer();
            final Map<K, V> kvMap = modelTable.multiGetOrInit(keyList).get();
            pullTracer.recordTime(kvMap.size());

            return kvMap;
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

  @Override
  public void push(final Map<K, P> keyToDeltaValueMap) {
    pushTracer.startTimer();
    modelTable.multiUpdateNoReply(keyToDeltaValueMap);
    pushTracer.recordTime(keyToDeltaValueMap.size());

    // update value in cache. this modification will not cause entry loading in cache.
    keyToDeltaValueMap.forEach((key, deltaValue) -> modelLoadingCache.asMap().
        computeIfPresent(key, (k, oldValue) -> modelUpdateFunction.updateValue(k, oldValue, deltaValue)));
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
  public List<V> pull(final List<K> keys, final Table<K, V, P> aModelTable) {
    try {
      final Map<K, V> result = aModelTable.multiGetOrInit(keys).get();

      final List<V> valueList = new ArrayList<>(keys.size());
      keys.forEach(key -> valueList.add(result.get(key)));

      return valueList;

    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
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
