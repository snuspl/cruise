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
import edu.snu.cay.services.ps.worker.api.ParameterWorker;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An {@link ModelAccessor} implementation based on PS.
 */
public class PSModelAccessor<K, P, V> implements ModelAccessor<K, P, V> {

  private final ParameterWorker<K, P, V> parameterWorker;

  private final Tracer pushTracer = new Tracer();
  private final Tracer pullTracer = new Tracer();

  @Inject
  PSModelAccessor(final ParameterWorker<K, P, V> parameterWorker) {
    this.parameterWorker = parameterWorker;
  }

  @Override
  public void push(final K key, final P deltaValue) {
    pushTracer.startTimer();
    parameterWorker.push(key, deltaValue);
    pushTracer.recordTime(1);
  }

  @Override
  public V pull(final K key) {
    pullTracer.startTimer();
    final V result = parameterWorker.pull(key);

    pullTracer.recordTime(1);
    return result;
  }

  @Override
  public List<V> pull(final List<K> keys) {
    pullTracer.startTimer();
    final List<V> resultValues = parameterWorker.pull(keys);
    
    pullTracer.recordTime(keys.size());
    return resultValues;
  }

  @Override
  public Map<String, Double> getAndResetMetrics() {
    final Map<String, Double> metrics = new HashMap<>();
    metrics.put(METRIC_TOTAL_PULL_TIME_SEC, pullTracer.totalElapsedTime());
    metrics.put(METRIC_AVG_PULL_TIME_SEC, pullTracer.avgTimePerElem());
    metrics.put(METRIC_TOTAL_PUSH_TIME_SEC, pushTracer.totalElapsedTime());
    metrics.put(METRIC_AVG_PUSH_TIME_SEC, pushTracer.avgTimePerElem());

    pullTracer.resetTrace();
    pushTracer.resetTrace();

    return metrics;
  }
}
