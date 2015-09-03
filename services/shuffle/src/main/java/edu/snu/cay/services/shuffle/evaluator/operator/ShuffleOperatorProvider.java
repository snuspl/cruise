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

import javax.inject.Inject;

/**
 * Provide shuffle sender and receiver.
 * If the end point is not a sender or a receiver, sender or receiver is set to null.
 */
public final class ShuffleOperatorProvider<K, V> {

  private final ShuffleSender<K, V> sender;
  private final ShuffleReceiver<K, V> receiver;

  @Inject
  private ShuffleOperatorProvider(final ShuffleSender<K, V> sender, final ShuffleReceiver<K, V> receiver) {
    this.sender = sender;
    this.receiver = receiver;
  }

  @Inject
  private ShuffleOperatorProvider(final ShuffleReceiver<K, V> receiver) {
    this.sender = null;
    this.receiver = receiver;
  }

  @Inject
  private ShuffleOperatorProvider(final ShuffleSender<K, V> sender) {
    this.sender = sender;
    this.receiver = null;
  }

  /**
   * @return a sender
   */
  public ShuffleSender<K, V> getSender() {
    return sender;
  }

  /**
   * @return a receiver
   */
  public ShuffleReceiver<K, V> getReceiver() {
    return receiver;
  }
}
