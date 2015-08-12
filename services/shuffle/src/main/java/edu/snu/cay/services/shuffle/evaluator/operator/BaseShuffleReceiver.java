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

import edu.snu.cay.services.shuffle.evaluator.operator.impl.BaseShuffleReceiverImpl;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

// TODO (#82) : BaseShuffleReceiver will be renamed to TupleMessageReceiver
// that does not implement ShuffleReceiver interface.
/**
 * Shuffle receiver that has base operation to receive tuples.
 *
 * Users have to register event handler for ShuffleTupleMessage to receive
 * tuples from senders.
 */
@DefaultImplementation(BaseShuffleReceiverImpl.class)
public interface BaseShuffleReceiver<K, V> extends ShuffleReceiver<K, V> {

  /**
   * Register a message handler that receives tuples arriving at this receiver
   *
   * @param messageHandler event handler for ShuffleTupleMessage
   */
  void registerTupleMessageHandler(EventHandler<Message<ShuffleTupleMessage<K, V>>> messageHandler);

}
