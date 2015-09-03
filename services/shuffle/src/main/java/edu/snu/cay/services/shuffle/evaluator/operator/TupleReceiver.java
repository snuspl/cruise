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

import edu.snu.cay.services.shuffle.network.ShuffleTupleMessageHandler;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Receiver that has base operation to receive tuples.
 *
 * ESNetworkSetup should be injected first to use this class.
 *
 * Users have to register event handler for ShuffleTupleMessage to receive
 * tuples from senders.
 */
public final class TupleReceiver<K, V> {

  private final ShuffleTupleMessageHandler<K, V> shuffleTupleMessageHandler;

  @Inject
  private TupleReceiver(
      final ShuffleTupleMessageHandler<K, V> shuffleTupleMessageHandler) {
    this.shuffleTupleMessageHandler = shuffleTupleMessageHandler;
  }

  /**
   * Set a message handler that receives tuples arriving at this receiver.
   *
   * @param messageHandler event handler for ShuffleTupleMessage
   */
  public void setTupleMessageHandler(final EventHandler<Message<Tuple<K, V>>> messageHandler) {
    shuffleTupleMessageHandler.setTupleMessageHandler(messageHandler);
  }
}
