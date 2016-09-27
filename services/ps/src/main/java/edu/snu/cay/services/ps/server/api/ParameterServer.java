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
package edu.snu.cay.services.ps.server.api;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.htrace.TraceInfo;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A Parameter Server interface that serves requests using several partitions.
 * Receives push and pull operations from (e.g., from the network) and immediately queues them.
 * The processing loop in each thread applies these operations in order; for pull operations
 * this results in a send call via {@link ServerSideMsgSender}.
 */
@EvaluatorSide
public interface ParameterServer<K, P, V> {

  /**
   * Process a {@code preValue} sent from a worker and store the resulting value.
   * Uses {@link edu.snu.cay.services.ps.server.api.ParameterUpdater} to generate a value from {@code preValue} and
   * to apply the generated value to the k-v store.
   *
   * The push operation is enqueued to the queue that is assigned to its partition and returned immediately.
   *
   * @param key key object that {@code preValue} is associated with
   * @param preValue preValue sent from the worker
   * @param keyHash hash of the key, a positive integer used to map to the correct partition
   */
  void push(final K key, final P preValue, final int keyHash);

  /**
   * Reply to srcId via {@link ServerSideMsgSender}
   * with the value corresponding to the key.
   *
   * The pull operation is enqueued to the queue that is assigned to its partition and returned immediately.
   *
   * @param key key object that the requested {@code value} is associated with
   * @param srcId network Id of the requester
   * @param keyHash hash of the key, a positive integer used to map to the correct partition
   * @param requestId pull request id assigned by ParameterWorker
   * @param traceInfo Information for Trace
   */
  void pull(final K key, final String srcId, final int keyHash, final int requestId,
            @Nullable final TraceInfo traceInfo);

  /**
   * Counts the number of pending operations.
   * Note that this method is only for testing purpose.
   * @return number of operations pending, on all queues
   */
  int opsPending();

  /**
   * Close the server, after waiting at most {@code timeoutMs} milliseconds
   * for queued messages to be handled.
   */
  void close(long timeoutMs) throws InterruptedException, TimeoutException, ExecutionException;
}
