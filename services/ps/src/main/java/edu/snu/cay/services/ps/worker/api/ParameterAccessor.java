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
package edu.snu.cay.services.ps.worker.api;

import org.apache.reef.annotations.audience.EvaluatorSide;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A Parameter accessor for a worker(task) thread.
 * This interacts with local caches(thread, worker) and the servers to provide or fetch parameters.
 * This is used to connect the a worker thread
 * to a {@link edu.snu.cay.services.ps.worker.impl.SSPParameterWorkerImpl}.
 *
 * @param <K> class type of parameter keys
 * @param <P> class type of parameter values before they are processed at the servers
 * @param <V> class type of parameter values after they are processed at the servers
 */
@EvaluatorSide
public interface ParameterAccessor<K, P, V> {

  /**
   * Update a {@code preValue} associated with a certain {@code key} to the thread cache.
   * Each {@code preValue} of same {@code key} is accumulated and sent to worker cache
   * and servers on {@code flush()} call.
   *
   * @param key      key object representing what is being updated
   * @param preValue value to push to the thread cache
   */
  void push(final K key, final P preValue);

  /**
   * Send cached keys and accumulated values in the thread cache to the servers.
   */
  void flush();

  /**
   * Fetch a value associated with a certain {@code key} from the servers or local caches(thread, worker).
   * Update local caches with {@code key}, value and global minimum clock when the value is fetched from the servers.
   *
   * @param key key object representing the expected value
   * @return value specified by the {@code key}, or {@code null} if something unexpected happens (see implementation)
   */
  V pull(final K key);

  /**
   * Fetch values associated with certain {@code keys} from the servers or local caches(thread, worker).
   * Update local caches with each key of {@code keys},
   * value and global minimum clock when values are fetched from the servers.
   *
   * @param keys a list of key objects representing the expected values
   * @return a list of values specified by the given {@code keys}. Some positions can be {@code null}
   * if something unexpected happens. (see implementation)
   */
  List<V> pull(final List<K> keys);

  /**
   * Close the worker, after waiting a maximum of {@code timeoutMs} milliseconds
   * for queued messages to be sent.
   */
  void close(final long timeoutMs) throws InterruptedException, TimeoutException, ExecutionException;
}
