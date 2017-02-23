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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.evaluator.api.Block;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Set of key-value pairs from a table.
 * Data migration between executors takes place with granularity of a block.
 */
final class BlockImpl<K, V> implements Block<K, V> {
  /**
   * This map serves as a collection of data in a block.
   */
  private final ConcurrentMap<K, V> subDataMap = new ConcurrentHashMap<>();

  /**
   * Update function for the table.
   */
  private final UpdateFunction<K, V> updateFunction;

  BlockImpl(final UpdateFunction<K, V> updateFunction) {
    this.updateFunction = updateFunction;
  }

  @Override
  public V put(final K key, final V value) {
    return subDataMap.put(key, value);
  }

  @Override
  public V get(final K key) {
    return subDataMap.get(key);
  }

  @Override
  public V update(final K key, final V deltaValue) {
    return subDataMap.compute(key, (k, v) -> {
      final V oldValue = (v == null) ? updateFunction.initValue(k) : v;
      return updateFunction.updateValue(key, oldValue, deltaValue);
    });
  }

  @Override
  public V remove(final K key) {
    return subDataMap.remove(key);
  }

  @Override
  public Map<K, V> getAll() {
    return new HashMap<>(subDataMap);
  }

  @Override
  public void putAll(final Map<K, V> data) {
    subDataMap.putAll(data);
  }

  @Override
  public Map<K, V> removeAll() {
    final Map<K, V> output = getAll();
    subDataMap.clear();
    return output;
  }

  @Override
  public int getNumPairs() {
    return subDataMap.size();
  }
}
