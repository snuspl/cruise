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
package edu.snu.cay.services.ps.worker.api;

import org.apache.reef.annotations.audience.EvaluatorSide;

/**
 * A Parameter Server worker (client) that interacts with the server to provide or fetch parameters.
 * Works as a set with {@link edu.snu.cay.services.ps.server.api.ParameterServer}.
 * @param <K> class type of parameter keys
 * @param <P> class type of parameter values before they are processed at the server
 * @param <V> class type of parameter values after they are processed at the server
 */
@EvaluatorSide
public interface ParameterWorker<K, P, V> {

  /**
   * Send a {@code preValue} associated with a certain {@code key} to the server.
   * @param key key object representing what is being sent
   * @param preValue value to push to the server
   */
  void push(K key, P preValue);

  /**
   * Fetch a value associated with a certain {@code key} from the server.
   * @param key key object representing the expected value
   * @return value specified by the {@code key}, or {@code null} if something unexpected happens (see implementation)
   */
  V pull(K key);

  /**
   * Reply to the worker with a {@code value} that was previously requested by {@code pull}.
   * @param key key object representing what was sent
   * @param value value sent from the server
   */
  void processReply(K key, V value);
}

