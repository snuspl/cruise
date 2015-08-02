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

import edu.snu.cay.services.shuffle.network.ShuffleTupleLinkListener;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessageCodec;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessageHandler;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * ContextStartHandler which registers connection factory for ShuffleTupleMessage.
 */
public final class ShuffleContextStartHandler implements EventHandler<ContextStart> {

  private final NetworkConnectionService networkConnectionService;
  private final Identifier tupleMessageConnectionId;
  private final ShuffleTupleMessageCodec tupleCodec;
  private final ShuffleTupleMessageHandler tupleHandler;
  private final ShuffleTupleLinkListener tupleLinkListener;

  @Inject
  private ShuffleContextStartHandler(
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      final NetworkConnectionService networkConnectionService,
      final ShuffleTupleMessageCodec tupleCodec,
      final ShuffleTupleMessageHandler tupleHandler,
      final ShuffleTupleLinkListener tupleLinkListener) {
    this.networkConnectionService = networkConnectionService;
    this.tupleMessageConnectionId = idFactory.getNewInstance(ShuffleParameters.NETWORK_CONNECTION_SERVICE_ID);
    this.tupleCodec = tupleCodec;
    this.tupleHandler = tupleHandler;
    this.tupleLinkListener = tupleLinkListener;
  }

  @Override
  public void onNext(final ContextStart value) {
    try {
      networkConnectionService
          .registerConnectionFactory(tupleMessageConnectionId, tupleCodec, tupleHandler, tupleLinkListener);
    } catch (final NetworkException e) {
      throw new RuntimeException(e);
    }
  }
}
