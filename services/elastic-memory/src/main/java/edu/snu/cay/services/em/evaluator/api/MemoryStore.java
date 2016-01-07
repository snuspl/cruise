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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Evaluator-side interface of MemoryStore, which provides a key-value style store,
 * whose data can be moved around the evaluator by the system.
 * The store can grow or shrink, depending on the status of this evaluator.
 */
@EvaluatorSide
public interface MemoryStore {

  /**
   * Register a data item of a certain data type to this store.
   *
   * @param dataType string that represents a certain data type
   * @param id global unique identifier of item
   * @param value data item to register
   * @param <T> actual data type
   */
  <T> void put(String dataType, long id, T value);

  /**
   * Register data items of a certain data type to this store.
   *
   * @param dataType string that represents a certain data type
   * @param ids list of global unique identifiers for each item
   * @param values list of data items to register
   * @param <T> actual data type
   */
  <T> void putList(String dataType, List<Long> ids, List<T> values);

  /**
   * Fetch a certain data item from this store.
   *
   * @param dataType string that represents a certain data type
   * @param id global unique identifier of item
   * @param <T> actual data type
   * @return a {@link Pair} of the data id and the actual data item,
   *         or {@code null} if no item is associated with the given id
   */
  <T> Pair<Long, T> get(String dataType, long id);

  /**
   * Fetch all data of a certain data type from this store.
   * The returned list is a shallow copy of the internal data structure.
   *
   * @param dataType string that represents a certain data type
   * @param <T> actual data type
   * @return a map of data ids and the corresponding data items.
   *         This map may be empty if no items of {@code dataType} are present.
   */
  <T> Map<Long, T> getAll(String dataType);

  /**
   * Fetch data items from this store whose ids are between {@code startId} and {@code endId}, both inclusive.
   * The returned list is a shallow copy of the internal data structure.
   *
   * @param dataType string that represents a certain data type
   * @param startId minimum value of the ids of items to fetch
   * @param endId maximum value of the ids of items to fetch
   * @param <T> actual data type
   * @return a map of data ids and the corresponding data items.
   *         This map may be empty if no items meet the given conditions.
   */
  <T> Map<Long, T> getRange(String dataType, long startId, long endId);

  /**
   * Fetch and remove a certain data item from this store.
   *
   * @param dataType string that represents a certain data type
   * @param id global unique identifier of item
   * @param <T> actual data type
   * @return a {@link Pair} of the data id and the actual data item,
   *         or {@code null} if no item is associated with the given id
   */
  <T> Pair<Long, T> remove(String dataType, long id);

  /**
   * Fetch and remove all data of a certain data type from this store.
   * A {@code getAll(dataType)} after this method will return null.
   *
   * @param dataType string that represents a certain data type
   * @param <T> actual data type
   * @return a map of data ids and the corresponding data items.
   *         This map may be empty if no items of {@code dataType} are present.
   */
  <T> Map<Long, T> removeAll(String dataType);

  /**
   * Fetch and remove data items from this store
   * whose ids are between {@code startId} and {@code endId}, both inclusive.
   *
   * @param dataType string that represents a certain data type
   * @param startId minimum value of the ids of items to fetch
   * @param endId maximum value of the ids of items to fetch
   * @param <T> actual data type
   * @return a map of data ids and the corresponding data items.
   *         This map may be empty if no items meet the given conditions.
   */
  <T> Map<Long, T> removeRange(String dataType, long startId, long endId);

  /**
   * Fetch all data types of items residing in this store, without duplicates.
   *
   * @return a set of strings that indicate the data types in this store
   */
  Set<String> getDataTypes();

  /**
   * Fetch the number of items associated with a certain data type.
   *
   * @param dataType string that represents a certain data type
   * @return number of items associated with {@code dataType}
   */
  int getNumUnits(String dataType);
}
