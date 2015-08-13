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
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleSender;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

/**
 * Evaluator side interface which communicates with corresponding ShuffleManager in driver,
 * and also provides shuffle operators to users.
 */
@EvaluatorSide
public interface Shuffle<K, V> {

  /**
   * Return the ShuffleReceiver for the shuffle.
   *
   * It throws RuntimeException if the current end point is not a receiver for the shuffle.
   *
   * @return shuffle receiver
   */
  <T extends ShuffleReceiver<K, V>> T getReceiver();

  /**
   * Return the ShuffleSender for the shuffle named shuffleName.
   *
   * It throws RuntimeException if the current end point is not a sender for the shuffle.
   *
   * @return shuffle sender
   */
  <T extends ShuffleSender<K, V>> T getSender();

  /**
   * Wait for a ShuffleControlMessage with code. All threads will be notified when the
   * ShuffleControlMessage arrives. It returns Optional.empty if the caller does not need
   * to be wait or there is an exceptional case.
   *
   * @param code a code for an expected ShuffleControlMessage
   * @return the ShuffleControlMessage
   */
  Optional<ShuffleControlMessage> waitForControlMessage(int code);

  /**
   * @return a shuffle description
   */
  ShuffleDescription getShuffleDescription();

  /**
   * @return an event handler for shuffle control messages.
   */
  EventHandler<Message<ShuffleControlMessage>> getControlMessageHandler();

  /**
   * @return a link listener for shuffle control messages.
   */
  LinkListener<Message<ShuffleControlMessage>> getControlLinkListener();
}
