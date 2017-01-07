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
package edu.snu.cay.services.em.evaluator.api;

import java.util.Map;

/**
 * A class representing Block which is an unit of EM's move.
 * It has a internal data structure to store key-value data.
 * @param <K> a type of key
 * @param <V> a type of value
 */
public interface Block<K, V> {

  /**
   * Returns all data in a block.
   * It is for supporting getAll method of MemoryStore.
   */
  Map<K, V> getAll();

  /**
   * Puts all data from the given Map to the block.
   */
  void putAll(Map<K, V> toPut);

  /**
   * Removes all data in a block.
   * It is for supporting removeAll method of MemoryStore.
   */
  Map<K, V> removeAll();

  /**
   * Returns the number of data in a block.
   * It is for supporting getNumUnits method of MemoryStore.
   */
  int getNumUnits();
}
