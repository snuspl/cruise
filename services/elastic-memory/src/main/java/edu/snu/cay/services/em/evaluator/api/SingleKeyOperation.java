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

import org.apache.reef.util.Optional;

/**
 * An interface for single key operations extending DataOperation.
 * @param <K> a type of data key
 * @param <V> a type of data value
 */
public interface SingleKeyOperation<K, V> extends DataOperation {

  /**
   * @return a data key
   */
  K getKey();

  /**
   * Returns an Optional with a data value.
   * It is empty for GET or REMOVE operations.
   * @return an Optional with a data value
   */
  Optional<V> getValue();

  /**
   * Commits the result of the operation and triggers the return of {@link #waitRemoteOps(long)} method.
   * @param output an output data of the operation
   * @param isSuccess true if the operation finished successfully
   */
  void commitResult(Optional<V> output, boolean isSuccess);

  /**
   * Returns an Optional with output data of the operation.
   * It returns an empty for PUT operation.
   * @return an Optional with the output data
   */
  Optional<V> getOutputData();

  /**
   * @return true if the operation finished successfully
   */
  boolean isSuccess();
}
