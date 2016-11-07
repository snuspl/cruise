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

import org.apache.reef.io.network.util.Pair;
import org.apache.reef.util.Optional;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * An interface for range key operations extending DataOperation.
 * @param <K> a type of data key
 * @param <V> a type of data value
 */
public interface RangeKeyOperation<K, V> extends DataOperation {

  /**
   * @return a range of data keys
   */
  List<Pair<K, K>> getDataKeyRanges();

  /**
   * Returns an Optional with the map of input data keys and its values for PUT operation.
   * It returns an empty Optional for GET and REMOVE operations.
   * @return an Optional with the map of input keys and its values
   */
  Optional<NavigableMap<K, V>> getDataKVMap();

  /**
   * Sets the total number of sub operations that {@link #waitRemoteOps(long)} will wait.
   * So it should be called before {@link #waitRemoteOps(long)}.
   * @param numSubOps the total number of sub operations
   */
  void setNumSubOps(int numSubOps);

  /**
   * Commits the result of a sub operation and returns the number of remaining sub operations.
   * When all the sub operations of the operation is finished,
   * it triggers the return of {@link #waitRemoteOps(long)} method.
   * @param output an output data of the sub operation
   * @param failedRangeList a list of failed key ranges of the sub operation
   * @return the number of remaining sub operations
   */
  int commitResult(Map<K, V> output, List<Pair<K, K>> failedRangeList);

  /**
   * Returns an aggregated output data of the operation.
   * It returns an empty map for PUT operation.
   * @return a map of the output data
   */
  Map<K, V> getOutputData();

  /**
   * @return a list of key ranges that the sub operations failed to execute.
   */
  List<Pair<K, K>> getFailedKeyRanges();
}
