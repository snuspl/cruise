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
package edu.snu.cay.services.et.evaluator.api;

import edu.snu.cay.services.et.evaluator.impl.TableImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Abstraction for access to collection of key-value pairs.
 *
 * @param <K> type of the key for this table.
 * @param <V> type of the value for this table.
 */
@DefaultImplementation(TableImpl.class)
public interface Table<K, V> {
  /**
   * Associates the specified value with the specified key.
   * @param key key with which value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with the key, or {@code null} if there was no mapping for the key
   */
  V put(K key, V value);

  /**
   * Returns the value to which the specified key is associated,
   * or {@code null} if this table contains no value for the key.
   * @param key the key whose associated value is to be returned
   * @return the value to which the specified key is associated,
   *         or {@code null} if no value is associated with the given key
   */
  V get(K key);

  /**
   * Update a value associated with the specified key using {@link UpdateFunction}.
   * Specifically, it processes the value associated with key with {@code deltaValue} by
   * {@link UpdateFunction#updateValue(K, V)}.
   * If there's no associated value, it initializes the value with {@link UpdateFunction#initValue(K)}.
   * To use this update method, users should provide their own implementation of {@link UpdateFunction}
   * and bind it to the interface.
   *
   * @param key global unique identifier of item
   * @param deltaValue value
   * @return the updated value associated with the key
   */
  V update(K key, V deltaValue);
}
