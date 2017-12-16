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
package edu.snu.spl.cruise.services.et.evaluator.api;

import org.apache.reef.annotations.audience.Private;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Interface for managing multiple key-value pairs with granularity of a block.
 */
@Private
public interface Block<K, V, U> extends Iterable<Entry<K, V>> {

  /**
   * @return a block Id
   */
  int getId();

  /**
   * Associates the specified value with the specified key.
   * @param key key with which value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with the key, or {@code null} if there was no mapping for the key
   */
  V put(K key, V value);

  /**
   * If the specified key is not already associated
   * with a value, associate it with the given value.
   * @param key key with which value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with the key, or {@code null} if there was no mapping for the key
   */
  V putIfAbsent(K key, V value);

  /**
   * Returns the value to which the specified key is associated,
   * or {@code null} if this table contains no value for the key.
   * @param key the key whose associated value is to be returned
   * @return the value to which the specified key is associated,
   *         or {@code null} if no value is associated with the given key
   */
  V get(K key);

  /**
   * Returns the value to which the specified key is associated.
   * If there is no mapping for the key, it returns a value of {@link UpdateFunction#initValue(K)}
   * after associating this value with the key.
   * @param key the key whose associated value is to be returned
   * @return the value to which the specified key is associated,
   *         or a value obtained by {@link UpdateFunction#initValue(K)} if there is no mapping for the key
   */
  V getOrInit(K key);

  /**
   * Update a value associated with the specified key using {@link UpdateFunction}.
   * Specifically, it processes the value associated with key with {@code updateValue} by
   * {@link UpdateFunction#updateValue(K, V, V)}.
   * If there's no associated value, it uses the value from {@link UpdateFunction#initValue(K)} as oldValue.
   *
   * @param key global unique identifier of item
   * @param updateValue update value
   * @return the updated value associated with the key
   */
  V update(K key, U updateValue);

  /**
   * Removes association for the specified key.
   * @param key key with which value is to be deleted
   * @return the previous value associated with the key, or {@code null} if there was no mapping for the key
   */
  V remove(K key);

  /**
   * Return all data in a block.
   */
  Map<K, V> getAll();

  /**
   * Put all data from the given map to a block.
   */
  void putAll(Map<K, V> data);

  /**
   * Remove all data in a block.
   * The block will be empty after this call returns.
   */
  void clear();

  /**
   * Return the number of data in a block.
   */
  int getNumPairs();
}
