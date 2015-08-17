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
package edu.snu.cay.services.shuffle.evaluator;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessageHandler;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Data receiver that has base operation to receive tuples.
 *
 * Users have to register event handler for ShuffleTupleMessage to receive
 * tuples from senders.
 */
public final class DataReceiver<K, V> {

  private final String shuffleName;
  private final ShuffleTupleMessageHandler shuffleTupleMessageHandler;

  @Inject
  private DataReceiver(
      final ShuffleDescription shuffleDescription,
      final ShuffleTupleMessageHandler shuffleTupleMessageHandler) {
    this.shuffleName = shuffleDescription.getShuffleName();
    this.shuffleTupleMessageHandler = shuffleTupleMessageHandler;
  }

  /**
   * Register a message handler that receives tuples arriving at this receiver.
   *
   * @param messageHandler event handler for ShuffleTupleMessage
   */
  public void registerTupleMessageHandler(final EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler) {
    shuffleTupleMessageHandler.registerMessageHandler(shuffleName, (EventHandler) messageHandler);
  }
}
