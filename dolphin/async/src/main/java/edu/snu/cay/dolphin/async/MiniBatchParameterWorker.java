/*
 * Copyright (C) 2016 Seoul National University
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

import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Store or provide parameters locally and interacts with {@link ParameterWorker} when each mini-batch ends.
 *
 * @param <K> class type of parameter keys
 * @param <P> class type of parameter values before they are processed at the servers
 * @param <V> class type of parameter values after they are processed at the server
 */
@NotThreadSafe
public final class MiniBatchParameterWorker<K, P, V> {
  private final ParameterUpdater<K, P, V> parameterUpdater;

  private final ParameterWorker<K, P, V> parameterWorker;

  /**
   * Store and update parameters locally, this is flushed when flushLocalUpdates is called.
   */
  private final Map<K, V> localParameters;

  /**
   * Store aggregate pre-values locally, this is flushed when flushLocalUpdates is called.
   */
  private final Map<K, P> aggregatedParameters;

  @Inject
  private MiniBatchParameterWorker(final ParameterUpdater<K, P, V> parameterUpdater,
                                  final ParameterWorker<K, P, V> parameterWorker) {
    this.parameterUpdater = parameterUpdater;
    this.parameterWorker = parameterWorker;
    this.localParameters = new HashMap<>();
    this.aggregatedParameters = new HashMap<>();
  }

  public V pull(final K key) {
    V value = localParameters.get(key);
    if (value == null) {
      value = parameterWorker.pull(key);
    }
    localParameters.put(key, value);
    return value;
  }

  public void push(final K key, final P preValue) {
    final V deltaValue = parameterUpdater.process(key, preValue);
    final V oldValue = localParameters.get(key);
    if (oldValue != null) {
      localParameters.put(key, parameterUpdater.update(oldValue, deltaValue));
    } else {
      localParameters.put(key, deltaValue);
    }

    final P oldPreValue = aggregatedParameters.get(key);
    if (oldPreValue != null) {
      aggregatedParameters.put(key, parameterUpdater.aggregate(oldPreValue, preValue));
    } else {
      aggregatedParameters.put(key, preValue);
    }
  }

  public List<V> pull(final List<K> keys) {
    final List<K> nonLocalKeys = new ArrayList<>();
    final List<V> valueList = new ArrayList();
    for (final K key : keys) {
      if (!localParameters.containsKey(key)) {
        nonLocalKeys.add(key);
      }
    }

    if (!nonLocalKeys.isEmpty()) {
      final List<V> remoteValues = parameterWorker.pull(nonLocalKeys);
      int i = 0;
      for (final K key : nonLocalKeys) {
        localParameters.put(key, remoteValues.get(i++));
      }
    }

    for (final K key : keys) {
      valueList.add(localParameters.get(key));
    }

    return valueList;
  }

  /**
   * Clear local parameter store and send aggregated pre-values to the server.
   */
  public void flushLocalUpdates() {
    localParameters.clear();
    for (final K key : aggregatedParameters.keySet()) {
      parameterWorker.push(key, aggregatedParameters.get(key));
    }
  }
}
