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
import org.htrace.TraceInfo;

import javax.annotation.Nullable;

/**
 * Process response messages received from the server.
 * This is an internal interface, to be used to connect the
 * {@link edu.snu.cay.services.ps.worker.impl.WorkerSideMsgHandler}
 * to a {@link edu.snu.cay.services.ps.worker.api.ParameterWorker}.
 */
@Private
public interface WorkerHandler<K, P, V> {
  /**
   * Reply to the worker with a {@code value} that was previously requested by {@link ParameterWorker#pull(Object)}.
   * @param key key object representing what was sent
   * @param value value sent from the server
   * @param requestId pull request id assigned by ParameterWorker
   * @param elapsedTimeInServer elapsed time since pull request's arrival at server
   * @param numReceivedBytes number of received data in bytes for response of a pull request
   * @param traceInfo Information for Trace
   */
  void processPullReply(K key, V value, int requestId, long elapsedTimeInServer, int numReceivedBytes,
                        @Nullable TraceInfo traceInfo);
}
