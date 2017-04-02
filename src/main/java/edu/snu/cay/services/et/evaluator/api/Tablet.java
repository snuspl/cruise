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

import edu.snu.cay.services.et.evaluator.impl.TabletImpl;
import edu.snu.cay.services.et.exceptions.BlockNotExistsException;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Abstraction representing a local portion of table.
 *
 * @param <K> type of the key for this table.
 * @param <V> type of the value for this table.
 * @param <U> type of the update value for this table.
 */
@DefaultImplementation(TabletImpl.class)
public interface Tablet<K, V, U> {
    /**
   * Associates the specified value with the specified key.
   * @param key key with which value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with the key, or {@code null} if there was no mapping for the key
   */
  V put(int blockId, K key, V value) throws BlockNotExistsException;

  /**
   * Associates the given value with the specified key only when the key is not associated with any value.
   * @param key key with which value is associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with the key, or {@code null} if there was no mapping for the key
   */
  V putIfAbsent(int blockId, K key, V value) throws BlockNotExistsException;

  /**
   * Returns the value to which the specified key is associated,
   * or {@code null} if this table contains no value for the key.
   * @param key the key whose associated value is to be returned
   * @return the value to which the specified key is associated,
   *         or {@code null} if no value is associated with the given key
   */
  V get(int blockId, K key) throws BlockNotExistsException;

  /**
   * Update a value associated with the specified key using {@link UpdateFunction}.
   * Specifically, it processes the value associated with key with {@code updateValue} by
   * {@link UpdateFunction#updateValue(K, V, U)}.
   * If there's no associated value, it uses the value from {@link UpdateFunction#initValue(K)} as oldValue.
   * To use this update method, users should provide their own implementation of {@link UpdateFunction}
   * and bind it to the interface.
   *
   * @param key global unique identifier of item
   * @param updateValue update value
   * @return the updated value associated with the key
   */
  V update(int blockId, K key, U updateValue) throws BlockNotExistsException;

  /**
   * Removes association for the specified key.
   * @param key key with which value is to be deleted
   * @return the previous value associated with the key, or {@code null} if there was no mapping for the key
   */
  V remove(int blockId, K key) throws BlockNotExistsException;

  /**
   * Returns a map that contains key-value mapping of all local data.
   * The returned map is a shallow copy of the internal data structure.
   * @return a map of data keys and the corresponding data values.
   *         This map may be empty if no data exists.
   */
  Map<K, V> getDataMap();

  /**
   * Gets an iterator of local data, which can react to background data migration.
   * Specifically, new items can be added to this iterator and the existing items can be deleted, for the items that
   * have not been consumed by this iterator yet.
   * Consistency of iterator depends on the implementation of Table.
   * @return an iterator of {@link Entry} of local data key-value pairs
   */
  Iterator<Entry<K, V>> getDataIterator();
}
