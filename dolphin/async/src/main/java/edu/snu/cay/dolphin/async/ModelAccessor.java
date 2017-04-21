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
package edu.snu.cay.dolphin.async;

import java.util.List;

/**
 * A class for accessing global model shared by multiple workers.
 * @param <K> type of keys
 * @param <P> type of delta values
 * @param <V> type of values
 */
public interface ModelAccessor<K, P, V> {

  /**
   * Updates a value associated with a {@code key} using a {@code deltaValue}.
   * @param key key of model parameter
   * @param deltaValue value to push to the servers
   */
  void push(K key, P deltaValue);

  /**
   * Fetches a value associated with a certain {@code key}.
   * @param key key of model parameter
   * @return value associated with the {@code key}, or {@code null} if there is no associated value
   */
  V pull(K key);

  /**
   * Fetches values associated with certain {@code keys}.
   * @param keys a list of keys of model parameter
   * @return a list of values associated with the given {@code keys}.
   *        Some positions in the list can be {@code null}, if the key has no associated value
   */
  List<V> pull(List<K> keys);
}
