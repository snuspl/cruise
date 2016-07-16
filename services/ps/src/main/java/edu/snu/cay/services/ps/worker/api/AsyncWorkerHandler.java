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

import org.apache.reef.annotations.audience.Private;

/**
 * Process response messages received from the server.
 * This is an internal interface, to be used to connect the
 * {@link edu.snu.cay.services.ps.worker.impl.WorkerSideMsgHandler}
 * to a {@link edu.snu.cay.services.ps.worker.api.ParameterWorker}.
 */
@Private
public interface AsyncWorkerHandler<K, P, V> {
  /**
   * Reply to the worker with a {@code value} that was previously requested by {@link ParameterWorker#pull(Object)}.
   * @param key key object representing what was sent
   * @param value value sent from the server
   */
  void processPullReply(K key, V value);

  /**
   * Notify the reject of Pull operation to the waiting worker thread.
   * @param key key object representing what was sent
   */
  void processPullReject(K key);

  /**
   * Retry the rejected Push operation.
   * @param key key object representing what was sent
   * @param preValue value to push to the servers
   */
  void processPushReject(K key, P preValue);
}
