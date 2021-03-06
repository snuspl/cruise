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

import edu.snu.spl.cruise.services.et.evaluator.impl.TableImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Abstraction for access to collection of key-value pairs distributed across executors.
 *
 * @param <K> type of the key for this table.
 * @param <V> type of the value for this table.
 * @param <U> type of the update value for this table.
 */
@DefaultImplementation(TableImpl.class)
public interface Table<K, V, U> {
  /**
   * Associates the specified value with the specified key.
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result when the request is complete.
   * Value should be non-null value.
   * @param key key with which value is to be associated
   * @param value value to be associated with the specified key
   * @return {@link Future} that will provide the previous value associated with the key,
   *         or {@code null} if there was no mapping for the key
   */
  Future<V> put(K key, @Nonnull V value);

  /**
   * See {@link #put}.
   * It returns immediately without waiting for the result.
   * Value should be non-null value.
   * @param key key with which value is to be associated
   * @param value value to be associated with the specified key
   */
  void putNoReply(K key, @Nonnull V value);

  /**
   * Associates the given value with the specified key only when the key is not associated with any value.
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result from the object when the request is complete.
   * Value should be non-null value.
   * @param key key with which value is to be associated
   * @param value value to be associated with the specified key
   * @return {@link Future} that will provide the previous value associated with the key,
   *         or {@code null} if there was no mapping for the key
   */
  Future<V> putIfAbsent(K key, @Nonnull V value);

  /**
   * See {@link #putIfAbsent}.
   * It returns immediately without waiting for the result.
   * @param key key with which value is associated
   * @param value value to be associated with the specified key
   * Value should be non-null value.
   */
  void putIfAbsentNoReply(K key, @Nonnull V value);

  /**
   * It is a multi-key version of {@link #put}.
   * Associates multiple key-value pairs.
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result from the object when the request is complete.
   * Values should be non-null value.
   * @param kvList a key-value pair list
   * @return {@link Future} that will provide the {@link Map} of specified keys and values that
   *         each key has previously associated value. If there are no previous values,
   *         map will be empty
   */
  Future<Map<K, V>> multiPut(List<Pair<K, V>> kvList);

  /**
   * Retrieves the value to which the specified key is associated,
   * or {@code null} if this table contains no value for the key.
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result from the object when the request is complete.
   * @param key key with which value is to be associated
   * @return {@link Future} that will provide the value to which the specified key is associated,
   *         or {@code null} if no value is associated with the given key
   */
  Future<V> get(K key);

  /**
   * Retrieves the values to which the specified keys are associated.
   * or {@code null} for the keys that this table contains no value.
   * It returns a {@link Future} of result,
   * which allows users to retrieve the result from the object when the request is complete.
   * @param keys keys with which values are to be associated
   * @return {@link Future} that will provide the map containing values to which the specified keys are associated,
   *         or {@code null} if no value is associated with the given keys
   */
  Future<Map<K, V>> multiGet(List<K> keys);

  /**
   * Retrieves the value to which the specified key is associated.
   * If this table contains no value for the key, it returns a value of {@link UpdateFunction#initValue(K)}
   * after associating this value with the key.
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result from the object when the request is complete.
   * @param key key with which value is to be associated
   * @return {@link Future} that will provide the value to which the specified key is associated,
   *         or a value obtained by {@link UpdateFunction#initValue(K)} if there is no mapping for the key
   */
  Future<V> getOrInit(K key);

  /**
   * Retrieves the values to which the specified keys are associated.
   * For the entries that this table has not added yet, {@link UpdateFunction#initValue(K)} are associated and inserted
   * to the table with the keys.
   * It returns a {@link Future} of result,
   * which allows users to retrieve the result from the object when the request is complete.
   * @param keys keys with which values are to be associated
   * @return {@link Future} that will provide the map containing values to which the specified keys are associated,
   *         or values obtained by {@link UpdateFunction#initValue(K)} for the keys that have no mapping
   */
  Future<Map<K, V>> multiGetOrInit(List<K> keys);

  /**
   * Update a value associated with the specified key using {@link UpdateFunction}.
   * Specifically, it processes the value associated with key with {@code updateValue} by
   * {@link UpdateFunction#updateValue(K, V, U)}.
   * If there's no associated value, it uses the value from {@link UpdateFunction#initValue(K)} as oldValue.
   * To use this update method, users should provide their own implementation of {@link UpdateFunction}
   * and bind it to the interface.
   *
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result from the object when the request is complete.
   * Update value should be non-null value.
   * @param key global unique identifier of item
   * @param updateValue update value
   * @return {@link Future} that will provide the updated value associated with the key
   */
  Future<V> update(K key, @Nonnull U updateValue);

  /**
   * See {@link #update}.
   * It returns immediately without waiting for the result.
   * Update value should be non-null value.
   * @param key global unique identifier of item
   * @param updateValue update value
   */
  void updateNoReply(K key, @Nonnull U updateValue);

  /**
   * It is a multi-key version of {@link #update}.
   * Update values associated with the specified keys using {@link UpdateFunction}.
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result from the object when the request is complete.
   * Note that it does not support updating null value to any key.
   * Update values should be non-null value.
   * @param kuMap a mapping between keys and updateValues
   * @return {@link Future} that will provide the {@link Map} which updated values associated with the specified keys.
   */
  Future<Map<K, V>> multiUpdate(Map<K, U> kuMap);

  /**
   * It is a multi-key version of {@link #update}.
   * It returns immediately without waiting for the result.
   * Update values associated with the specified keys using {@link UpdateFunction}.
   * Note that it does not support updating null value to any key.
   * Update values should be non-null value.
   * @param kuMap a mapping between keys and updateValues
   */
  void multiUpdateNoReply(Map<K, U> kuMap);

  /**
   * Removes association for the specified key.
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result from the object when the request is complete.
   * @param key key with which value is to be associated
   * @return {@link Future} that will provide the previous value associated with the key,
   *         or {@code null} if there was no mapping for the key
   */
  Future<V> remove(K key);

  /**
   * See {@link #remove}.
   * It returns immediately without waiting for the result.
   * @param key key with which value is to be deleted
   */
  void removeNoReply(K key);

  /**
   * @return a local {@link Tablet}
   */
  Tablet<K, V, U> getLocalTablet();
}
