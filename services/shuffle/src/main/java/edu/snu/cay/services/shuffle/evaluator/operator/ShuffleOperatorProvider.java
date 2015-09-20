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

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;

/**
 * Provide shuffle sender and receiver.
 *
 * If the end point is not a sender or a receiver, they are set to NullShuffleSender and
 * NullShuffleReceiver, respectively.
 *
 * Note that classes used in operators, such as ESControlMessageSender, should be initialized
 * and set needed parameters firstly before getting operators.
 *
 * For example, when a sender want to use ESControlMessageSender,
 * ControlMessageSetup.setControlMessageHandlerAndLinkListener should be called before a user get
 * the sender using getSender().
 */
public final class ShuffleOperatorProvider<K, V> {

  private final Injector injector;
  private ShuffleSender<K, V> sender;
  private ShuffleReceiver<K, V> receiver;

  @Inject
  private ShuffleOperatorProvider(final Injector injector) {
    this.injector = injector;
  }

  /**
   * @return a sender
   */
  public ShuffleSender<K, V> getSender() {
    if (sender == null) {
      try {
        sender = injector.forkInjector().getInstance(ShuffleSender.class);
      } catch (final InjectionException e) {
        throw new RuntimeException(e);
      }
    }

    return sender;
  }

  /**
   * @return a receiver
   */
  public ShuffleReceiver<K, V> getReceiver() {
    if (receiver == null) {
      try {
        receiver = injector.forkInjector().getInstance(ShuffleReceiver.class);
      } catch (final InjectionException e) {
        throw new RuntimeException(e);
      }
    }

    return receiver;
  }
}
