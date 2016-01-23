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
package edu.snu.cay.services.ps.worker.concurrent;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Worker-side Parameter Server message sender.
 */
@EvaluatorSide
@DefaultImplementation(WorkerSideMsgSenderImpl.class)
public interface WorkerSideMsgSender<K, P> {

  /**
   * Send a key-value pair to another evaluator.
   * @param destId Network Connection Service identifier of the destination evaluator
   * @param key key object representing what is being sent
   * @param preValue value to be sent to the destination
   */
  void sendPushMsg(final String destId, final K key, final P preValue);

  /**
   * Send a request to another evaluator for fetching a certain value.
   * After this message, a {@link edu.snu.cay.services.ps.avro.ReplyMsg} containing the requested value
   * should be sent from the destination as a reply.
   * @param destId Network Connection Service identifier of the destination evaluator
   * @param key key object representing the expected value
   */
  void sendPullMsg(final String destId, final K key);
}
