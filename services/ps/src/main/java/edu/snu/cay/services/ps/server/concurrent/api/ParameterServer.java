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
package edu.snu.cay.services.ps.server.concurrent.api;

import edu.snu.cay.services.ps.server.concurrent.impl.ValueEntry;
import org.apache.reef.annotations.audience.EvaluatorSide;

/**
 * A Parameter Server server that interacts with workers (clients) and stores parameters in the form of a k-v store.
 * Works as a set with {@link edu.snu.cay.services.ps.worker.api.ParameterWorker}.
 * @param <K> class type of parameter keys
 * @param <P> class type of parameter values before they are processed at the server
 * @param <V> class type of parameter values after they are processed at the server
 */
@EvaluatorSide
public interface ParameterServer<K, P, V> {

  /**
   * Store a {@code preValue} sent from a worker.
   * @param key key object that {@code preValue} is associated with
   * @param preValue preValue sent from the worker
   */
  void push(K key, P preValue);

  /**
   * Load and return a value requested by a worker.
   * @param key key object that the requested {@code value} is associated with
   * @return a {@code ValueEntry} object that contains the value requested by the worker
   */
  ValueEntry<V> pull(K key);
}
