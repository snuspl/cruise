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
package edu.snu.cay.services.em.evaluator.impl.singlekey;

import edu.snu.cay.services.em.evaluator.api.Block;
import edu.snu.cay.services.em.evaluator.api.EMUpdateFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Block class that has a {@code subDataMap}, which is an unit of EM's move.
 * Data is stored in a {@code ConcurrentHashMap} to maximize concurrency.
 */
final class BlockImpl<K, V> implements Block<K, V> {

  /**
   * The map serves as a collection of data in a Block.
   * Its implementation is {@code ConcurrentHashMap} to
   * maximize the performance of concurrent single-key operations.
   */
  private final ConcurrentMap<K, V> subDataMap = new ConcurrentHashMap<>();
  private final EMUpdateFunction<K, V> emUpdateFunction;


  BlockImpl(final EMUpdateFunction updateFunction) {
    this.emUpdateFunction = updateFunction;
  }

  void put(final K key, final V value) {
    subDataMap.put(key, value);
  }

  V get(final K key) {
    return subDataMap.get(key);
  }

  V remove(final K key) {
    return subDataMap.remove(key);
  }

  /**
   * Updates the value associated with the given {@code key} using {@code deltaValue}.
   */
  V update(final K key, final V deltaValue) {
    return subDataMap.compute(key, (k, v) -> {
      final V oldValue = (v == null) ? emUpdateFunction.getInitValue(k) : v;
      return emUpdateFunction.getUpdateValue(oldValue, deltaValue);
    });
  }

  @Override
  public Map<K, V> getAll() {
    return new HashMap<>(subDataMap);
  }

  @Override
  public void putAll(final Map<K, V> toPut) {
    subDataMap.putAll(toPut);
  }

  @Override
  public Map<K, V> removeAll() {
    final Map<K, V> output = new HashMap<>(subDataMap);
    subDataMap.clear();
    return output;
  }

  @Override
  public int getNumUnits() {
    return subDataMap.size();
  }
}
