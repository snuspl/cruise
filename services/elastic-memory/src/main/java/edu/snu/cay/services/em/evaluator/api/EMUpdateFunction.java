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
package edu.snu.cay.services.em.evaluator.api;

import edu.snu.cay.services.em.evaluator.impl.VoidUpdateFunction;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * An EM update function interface that provides an initial value for key
 * and an update value for given old value and delta value when {@link MemoryStore#update} is called.
 * Users should provide their own implementation, corresponding to their update semantic.
 * @param <K> a type of key
 * @param <V> a type of data
 */
@DefaultImplementation(VoidUpdateFunction.class)
public interface EMUpdateFunction<K, V> {

  /**
   * Gets an initial value to associate with given key in {@link MemoryStore#update},
   * when no value has been associated in the MemoryStore.
   * @param key a key
   * @return an initial value
   */
  V getInitValue(K key);

  /**
   * Gets an update value by applying deltaValue to oldValue when {@link MemoryStore#update} is called.
   * Implementations should specify how to update the associated value with the given deltaValue.
   * @param oldValue an old value
   * @param deltaValue a delta value
   * @return an update value
   */
  V getUpdateValue(V oldValue, V deltaValue);
}
