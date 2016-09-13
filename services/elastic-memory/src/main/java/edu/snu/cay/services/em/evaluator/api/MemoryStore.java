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
package edu.snu.cay.services.em.evaluator.api;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.util.Pair;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * Interface that provides a key-value style store,
 * whose data can be moved around the evaluators by the system.
 *
 * @param <K> type of the key (or id) for this MemoryStore.
 *           Range-related operations are available only when K has a total ordering.
 */
@EvaluatorSide
public interface MemoryStore<K> {

  /**
   * Register a data item to this store.
   * MemoryStore does not permit null for data values.
   *
   * @param id global unique identifier of item
   * @param value data item to register
   * @param <V> type of the data
   * @return a {@link Pair} of the data id and a boolean that is true when the operation succeeded
   */
  <V> Pair<K, Boolean> put(K id, @Nonnull V value);

  /**
   * Register data items to this store.
   *
   * @param ids list of global unique identifiers for each item
   * @param values list of data items to register
   * @param <V> type of the data
   * @return a map of data ids and booleans that are true when the operation succeeded for each id
   */
  <V> Map<K, Boolean> putList(List<K> ids, List<V> values);

  /**
   * Fetch a certain data item from this store.
   *
   * @param id global unique identifier of item
   * @param <V> type of the data
   * @return a {@link Pair} of the data id and the actual data item,
   *         or {@code null} if no item is associated with the given id
   */
  <V> Pair<K, V> get(K id);

  /**
   * Fetch all local data from this store.
   * The returned list is a shallow copy of the internal data structure.
   *
   * @param <V> type of the data
   * @return a map of data ids and the corresponding data items.
   *         This map may be empty if no item is present.
   */
  <V> Map<K, V> getAll();

  /**
   * Fetch data items from this store whose ids are between {@code startId} and {@code endId}, both inclusive.
   * The returned list is a shallow copy of the internal data structure.
   *
   * @param startId minimum value of the ids of items to fetch
   * @param endId maximum value of the ids of items to fetch
   * @param <V> type of the data
   * @return a map of data ids and the corresponding data items.
   *         This map may be empty if no items meet the given conditions.
   */
  <V> Map<K, V> getRange(K startId, K endId);

  /**
   * Update a data item in the store with {@code deltaValue} using {@link EMUpdateFunction}.
   * Specifically, it processes the value associated with key with {@code deltaValue} by
   * {@link EMUpdateFunction#getUpdateValue(Object, Object)}.
   * If there's no associated value, it initializes the value
   * with {@link EMUpdateFunction#getInitValue(Object)} and process it.
   * To use this update method, users should provide their own implementation of {@link EMUpdateFunction}
   * and bind it to the interface.
   *
   * @param id global unique identifier of item
   * @param deltaValue value
   * @param <V> type of the data
   * @return a {@link Pair} of the data id and the data item
   */
  <V> Pair<K, V> update(K id, V deltaValue);

  /**
   * Fetch and remove a certain data item from this store.
   *
   * @param id global unique identifier of item
   * @param <V> type of the data
   * @return a {@link Pair} of the data id and the actual data item,
   *         or {@code null} if no item is associated with the given id
   */
  <V> Pair<K, V> remove(K id);

  /**
   * Fetch and remove all local data from this store.
   * A {@code getAll()} after this method will return null.
   *
   * @param <V> type of the data
   * @return a map of data ids and the corresponding data items.
   *         This map may be empty if no item is present.
   */
  <V> Map<K, V> removeAll();

  /**
   * Fetch and remove data items from this store
   * whose ids are between {@code startId} and {@code endId}, both inclusive.
   *
   * @param startId minimum value of the ids of items to fetch
   * @param endId maximum value of the ids of items to fetch
   * @param <V> type of the data
   * @return a map of data ids and the corresponding data items.
   *         This map may be empty if no items meet the given conditions.
   */
  <V> Map<K, V> removeRange(K startId, K endId);

  /**
   * Register listener for block update event to this store.
   * @param listener a client-defined block update notification listener to be registered.
   * @return returns true if the listener is registered successfully.
   */
  boolean registerBlockUpdateListener(final BlockUpdateNotifyListener listener);

  /**
   * @return  number of blocks in the MemoryStore
   */
  int getNumBlocks();
}
