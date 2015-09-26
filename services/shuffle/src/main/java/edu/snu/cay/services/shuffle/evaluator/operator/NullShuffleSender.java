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

import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Default implementation of ShuffleSender.
 *
 * If a node is not a sender of a shuffle, this is injected as a sender,
 * and it throws a RuntimeException when a user try to use the sender.
 */
final class NullShuffleSender<K, V> implements ShuffleSender<K, V> {

  private final String shuffleName;
  private final String endPointId;

  @Inject
  private NullShuffleSender(
      @Parameter(ShuffleParameters.ShuffleName.class) final String shuffleName,
      @Parameter(ShuffleParameters.EndPointId.class) final String endPointId) {
    this.shuffleName = shuffleName;
    this.endPointId = endPointId;
  }

  @Override
  public void onControlMessage(final Message<ShuffleControlMessage> message) {
    throw new RuntimeException(endPointId + " is not a sender of " + shuffleName);
  }
}
