/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.aggregate;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

public final class AggregateNetworkSetup<T> {
  private static final String AGGREGATE_IDENTIFIER = "AGGREGATE";

  private final NetworkConnectionService networkConnectionService;
  private final Identifier connectionFactoryIdentifier;
  private final StreamingCodec<T> streamingCodec;
  private final EventHandler<Message<T>> handler;
  private ConnectionFactory<T> connectionFactory;

  // TODO: Use NamedParameters to receive streamingCodec and handler
  @Inject
  private AggregateNetworkSetup(final NetworkConnectionService networkConnectionService,
                                final IdentifierFactory identifierFactory,
                                final StreamingCodec<T> streamingCodec,
                                final EventHandler<Message<T>> handler) throws NetworkException {
    this.networkConnectionService = networkConnectionService;
    this.connectionFactoryIdentifier = identifierFactory.getNewInstance(AGGREGATE_IDENTIFIER);
    this.streamingCodec = streamingCodec;
    this.handler = handler;
  }

  public ConnectionFactory<T> registerConnectionFactory(final Identifier localEndPointId) {
    connectionFactory = networkConnectionService.registerConnectionFactory(connectionFactoryIdentifier,
        streamingCodec, handler, null, localEndPointId);
    return connectionFactory;
  }

  public void unregisterConnectionFactory() {
    networkConnectionService.unregisterConnectionFactory(connectionFactoryIdentifier);
  }

  public ConnectionFactory<T> getConnectionFactory() {
    if (connectionFactory == null) {
      throw new RuntimeException("A connection factory has not been registered yet.");
    }
    return connectionFactory;
  }
}