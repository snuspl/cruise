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
import edu.snu.cay.utils.MemoryUtils;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of {@link ModelAccessor} that handles push/pull requests using Elastic Tables (ET).
 * This component is responsible for collecting metrics, and is not thread-safe because the tracing components are not
 * thread-safe at the moment.
 */
@NotThreadSafe
public final class ETModelAccessor<K, P, V> implements ModelAccessor<K, P, V> {
  private static final Logger LOG = Logger.getLogger(ETModelAccessor.class.getName());

  private final Table<K, V, P> modelTable;

  private final Tracer pushTracer = new Tracer();
  private final Tracer pullTracer = new Tracer();

  @Inject
  private ETModelAccessor(@Parameter(DolphinParameters.ModelTableId.class) final String modelTableId,
                  final TableAccessor tableAccessor) throws TableNotExistException {
    this.modelTable = tableAccessor.getTable(modelTableId);
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

    final List<V> resultValues = pull(keys, modelTable);

    pullTracer.recordTime(keys.size());
    LOG.log(Level.INFO, "{0} keys have been pulled. Used memory: {1} MB",
        new Object[] {keys.size(), MemoryUtils.getUsedMemoryMB()});
    return resultValues;
  }

  /**
   * Do {@link #pull(List)} with a given table.
   * @param keys a list of keys of model parameter
   * @param aModelTable a table to read value from
   * @return a list of values associated with the given {@code keys}.
   *        Some positions in the list can be {@code null}, if the key has no associated value
   */
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
