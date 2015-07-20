/**
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
package edu.snu.cay.services.shuffle.impl;

import edu.snu.cay.services.shuffle.description.ShuffleGroupDescription;
import edu.snu.cay.services.shuffle.task.ShuffleGroupClient;
import edu.snu.cay.services.shuffle.task.TupleOperatorFactory;
import edu.snu.cay.services.shuffle.task.TupleReceiver;
import edu.snu.cay.services.shuffle.task.TupleSender;

import javax.inject.Inject;

/**
 * Simple implementation of ShuffleGroupClient. The initial shuffle group description never be changed.
 */
public final class FixedShuffleGroupClient implements ShuffleGroupClient {

  private final ShuffleGroupDescription initialShuffleGroupDescription;
  private final TupleOperatorFactory operatorFactory;

  @Inject
  private FixedShuffleGroupClient(
      final ShuffleGroupDescription initialShuffleGroupDescription,
      final TupleOperatorFactory operatorFactory) {
    this.initialShuffleGroupDescription = initialShuffleGroupDescription;
    this.operatorFactory = operatorFactory;
  }

  @Override
  public <K, V> TupleReceiver<K, V> getReceiver(final String shuffleName) {
    return operatorFactory.newTupleReceiver(initialShuffleGroupDescription.getShuffleDescription(shuffleName));
  }

  @Override
  public <K, V> TupleSender<K, V> getSender(final String shuffleName) {
    return operatorFactory.newTupleSender(initialShuffleGroupDescription.getShuffleDescription(shuffleName));
  }

  @Override
  public ShuffleGroupDescription getShuffleGroupDescription() {
    return initialShuffleGroupDescription;
  }
}
