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
package edu.snu.cay.services.et.evaluator.api;

import edu.snu.cay.services.et.evaluator.impl.TableImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.annotations.DefaultImplementation;

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
   * @param key key with which value is to be associated
   * @param value value to be associated with the specified key
   * @return {@link Future} that will provide the previous value associated with the key,
   *         or {@code null} if there was no mapping for the key
   */
  Future<V> put(K key, V value);

  /**
   * See {@link #put}.
   * It returns immediately without waiting for the result.
   * @param key key with which value is to be associated
   * @param value value to be associated with the specified key
   */
  void putNoReply(K key, V value);

  /**
   * Associates the given value with the specified key only when the key is not associated with any value.
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result from the object when the request is complete.
   * @param key key with which value is to be associated
   * @param value value to be associated with the specified key
   * @return {@link Future} that will provide the previous value associated with the key,
   *         or {@code null} if there was no mapping for the key
   */
  Future<V> putIfAbsent(K key, V value);

  /**
   * See {@link #putIfAbsent}.
   * It returns immediately without waiting for the result.
   * @param key key with which value is associated
   * @param value value to be associated with the specified key
   */
  void putIfAbsentNoReply(K key, V value);

  /**
   * It is a multi-key version of {@link #put}.
   * Associates multiple key-value pairs.
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result from the object when the request is complete.
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
   * Update a value associated with the specified key using {@link UpdateFunction}.
   * Specifically, it processes the value associated with key with {@code updateValue} by
   * {@link UpdateFunction#updateValue(K, V, U)}.
   * If there's no associated value, it uses the value from {@link UpdateFunction#initValue(K)} as oldValue.
   * To use this update method, users should provide their own implementation of {@link UpdateFunction}
   * and bind it to the interface.
   *
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result from the object when the request is complete.
   * @param key global unique identifier of item
   * @param updateValue update value
   * @return {@link Future} that will provide the updated value associated with the key
   */
  Future<V> update(K key, U updateValue);

  /**
   * It is a multi-key version of {@link #update}.
   * Update values associated with the specified keys using {@link UpdateFunction}.
   * It returns a {@link Future} of result, which
   * allows users to retrieve the result from the object when the request is complete.
   * Note that it doesn't support updating null value to any key.
   * @param kuList a key-updateValue pair list
   * @return {@link Future} that will provide the {@link Map} which updated values associated with the specified keys.
   */
  Future<Map<K, V>> multiUpdate(List<Pair<K, U>> kuList);

  /**
   * See {@link #update}.
   * It returns immediately without waiting for the result.
   * @param key global unique identifier of item
   * @param updateValue update value
   */
  void updateNoReply(K key, U updateValue);

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
