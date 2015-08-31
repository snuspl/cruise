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
package edu.snu.cay.services.shuffle.evaluator.operator;

import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.Message;

/**
 *
 */
public interface PushDataListener<K, V> {

  /**
   * Handle a message from a sender.
   *
   * @param message a tuple message
   */
  void onTupleMessage(Message<ShuffleTupleMessage<K, V>> message);

  /**
   * Handle the case where all senders completed to send data in one iteration.
   */
  void onComplete();

  /**
   * Handle the case where the receiver was shutdown by the manager.
   */
  void onShutdown();
}
