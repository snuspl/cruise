/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.ps.server.impl;

import edu.snu.cay.services.ps.server.ValueEntry;
import edu.snu.cay.services.ps.server.api.ParameterServer;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Parameter Server server that consists of exactly one node.
 * Users should spawn a single evaluator for this server.
 * This class is thread-safe if and only if {@link ParameterUpdater} is thread-safe.
 */
@EvaluatorSide
public final class SingleNodeParameterServer<K, P, V> implements ParameterServer<K, P, V> {

  /**
   * A map holding keys and values sent from workers.
   */
  private final ConcurrentMap<K, ValueEntry<V>> kvStore;

  /**
   * Object for processing preValues and applying updates to existing values.
   */
  private final ParameterUpdater<K, P, V> parameterUpdater;

  @Inject
  private SingleNodeParameterServer(final ParameterUpdater<K, P, V> parameterUpdater) {
    this.kvStore = new ConcurrentHashMap<>();
    this.parameterUpdater = parameterUpdater;
  }

  /**
   * Process a {@code preValue} sent from a worker and store the resulting value.
   * Uses {@link ParameterUpdater} to generate a value from {@code preValue} and to apply the generated value to
   * the k-v store. Updates are done only when a write lock is acquired, for concurrency between
   * multiple push/pull threads.
   * @param key key object that {@code preValue} is associated with
   * @param preValue preValue sent from the worker
   */
  @Override
  public void push(final K key, final P preValue) {
    if (!kvStore.containsKey(key)) {
      kvStore.putIfAbsent(key, new ValueEntry<>(parameterUpdater.initValue(key)));
    }

    final V deltaValue = parameterUpdater.process(key, preValue);
    if (deltaValue == null) {
      return;
    }

    final ValueEntry<V> oldValueEntry = kvStore.get(key);
    oldValueEntry.getReadWriteLock().writeLock().lock();
    try {
      // We cannot predict whether the updater will modify the value object itself or not.
      // Therefore we acquire the lock before the update starts.
      // If we can assume value objects are immutable then we might not even need a lock,
      // since the k-v store is already a ConcurrentMap.
      final V newValue = parameterUpdater.update(oldValueEntry.getValue(), deltaValue);
      oldValueEntry.setValue(newValue);
    } finally {
      oldValueEntry.getReadWriteLock().writeLock().unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ValueEntry<V> pull(final K key) {
    if (!kvStore.containsKey(key)) {
      kvStore.putIfAbsent(key, new ValueEntry<>(parameterUpdater.initValue(key)));
    }

    return kvStore.get(key);
  }
}
