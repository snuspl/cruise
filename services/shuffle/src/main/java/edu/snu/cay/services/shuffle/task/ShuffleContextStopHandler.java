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
package edu.snu.cay.services.shuffle.task;

import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * ContextStopHandler which un-registers connection factory for ShuffleTupleMessage.
 */
public final class ShuffleContextStopHandler implements EventHandler<ContextStop> {

  private final NetworkConnectionService networkConnectionService;
  private final Identifier tupleMessageConnectionId;

  @Inject
  private ShuffleContextStopHandler(
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      final NetworkConnectionService networkConnectionService) {

    this.networkConnectionService = networkConnectionService;
    this.tupleMessageConnectionId = idFactory.getNewInstance(ShuffleParameters.NETWORK_CONNECTION_SERVICE_ID);
  }

  @Override
  public void onNext(final ContextStop value) {
    networkConnectionService.unregisterConnectionFactory(tupleMessageConnectionId);
  }
}
