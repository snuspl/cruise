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
package edu.snu.cay.dolphin.core;

import edu.snu.cay.services.shuffle.evaluator.operator.PushDataListener;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;

import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

final class ShuffleDataReceiver implements PushDataListener {

  private static final Logger LOG = Logger.getLogger(ShuffleDataReceiver.class.getName());

  private final Queue<Tuple> receivedTupleQueue;

  ShuffleDataReceiver(final Queue<Tuple> receivedTupleQueue) {
    this.receivedTupleQueue = receivedTupleQueue;
  }

  @Override
  public void onTupleMessage(final Message message) {
    for (final Object tuple : message.getData()) {
      receivedTupleQueue.add((Tuple) tuple);
    }
  }

  @Override
  public void onComplete() {
    LOG.log(Level.INFO, "An shuffle iteration was completed");
  }

  @Override
  public void onShutdown() {
  }
}
