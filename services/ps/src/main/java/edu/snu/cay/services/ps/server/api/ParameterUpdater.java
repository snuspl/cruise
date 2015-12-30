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
package edu.snu.cay.services.ps.server.api;

/**
 * A Parameter Server updater that handles preValues sent from workers before they are stored at the server.
 * @param <K> class type of parameter keys
 * @param <P> class type of parameter values before they are processed at the server
 * @param <V> class type of parameter values after they are processed at the server
 */
public interface ParameterUpdater<K, P, V> {

  /**
   * Generate a value that matches the value type of the server
   * from {@code preValue}, that is associated with {@code key}.
   * @param key key object with which the generated value should be associated with
   * @param preValue preValue sent from a worker that will be used to create a new value
   * @return new value that was generated, or null if no update should occur
   */
  V process(K key, P preValue);

  /**
   * Create a new value using an existing entry from the server and a new value generated from a worker's preValue.
   * @param oldValue the current value stored at the server
   * @param deltaValue new value generated from a worker's preValue.
   * @return new value that was generated
   */
  V update(V oldValue, V deltaValue);

  /**
   * Generate a initial value for a certain {@code key}.
   * @param key key object with which the generated value should be associated with
   * @return the initial value that was generated
   */
  V initValue(K key);
}
