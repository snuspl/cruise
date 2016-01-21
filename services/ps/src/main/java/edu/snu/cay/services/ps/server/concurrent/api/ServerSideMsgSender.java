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

import edu.snu.cay.services.ps.server.concurrent.impl.ServerSideMsgSenderImpl;
import edu.snu.cay.services.ps.server.concurrent.impl.ValueEntry;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Server-side Parameter Server message sender.
 */
@EvaluatorSide
@DefaultImplementation(ServerSideMsgSenderImpl.class)
public interface ServerSideMsgSender<K, V> {

  /**
   * Send a reply to another evaluator that requested a certain value.
   * This message is a reply to a {@link edu.snu.cay.services.ps.avro.PullMsg}.
   * @param destId Network Connection Service identifier of the destination evaluator
   * @param key key object associated with the expected value
   * @param valueEntry {@link ValueEntry} object containing the requested value
   */
  void sendReplyMsg(final String destId, final K key, final ValueEntry<V> valueEntry);
}
