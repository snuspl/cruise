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

import org.htrace.TraceInfo;

import javax.annotation.Nullable;

/**
 * Sender for ParameterServer.
 */
public interface ServerSideMsgSender<K, P, V> {
  /**
   * Implementing classes must serialize K, V immediately within the calling thread,
   * to ensure atomicity of updates.
   * @param destId the destination's network address
   * @param key key, to be serialized immediately
   * @param value value, to be serialized immediately
   * @param requestId pull request id assigned by ParameterWorker
   * @param elapsedTimeInServer elapsed time since pull request's arrival at server
   * @param traceInfo Information for Trace
   */
  void sendPullReplyMsg(String destId, K key, V value, int requestId, long elapsedTimeInServer,
                        @Nullable final TraceInfo traceInfo);

  /**
   * Sends a push msg for {@code key} to a corresponding server.
   *
   * @param destId   an id of destination server
   * @param key      a key to push
   * @param preValue a previous value to push
   */
  void sendPushMsg(final String destId, final K key, final P preValue);

  /**
   * Sends a pull msg for {@code key} to a corresponding server.
   *
   * @param destId    an id of destination server
   * @param srcId     an id of requester
   * @param key       a key to pull
   * @param requestId pull request id assigned by ParameterWorker
   * @param traceInfo Information for Trace
   */
  void sendPullMsg(final String destId, final String srcId, final K key, final int requestId,
                   @Nullable final TraceInfo traceInfo);
}
