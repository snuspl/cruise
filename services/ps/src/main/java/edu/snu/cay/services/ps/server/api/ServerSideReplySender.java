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

/**
 * Sender for ParameterServer.
 */
public interface
ServerSideReplySender<K, P, V> {
  /**
   * Implementing classes must serialize K, V immediately within the calling thread,
   * to ensure atomicity of updates.
   * @param destId the destination's network address
   * @param key key, to be serialized immediately
   * @param value value, to be serialized immediately
   * @param elapsedTimeInServer elapsed time since pull request's arrival at server
   */
  void sendPullReplyMsg(String destId, K key, V value, long elapsedTimeInServer);

  /**
   * Send a reject msg for push operation to the worker who requested.
   * @param destId the destination's network address
   * @param key key object that {@code preValue} is associated with
   * @param preValue preValue sent from the worker
   */
  void sendPushRejectMsg(String destId, K key, P preValue);

  /**
   * Send a reject msg for pull operation to the worker who requested.
   * @param destId the destination's network address
   * @param key key object that the requested {@code value} is associated with
   */
  void sendPullRejectMsg(String destId, K key);
}
