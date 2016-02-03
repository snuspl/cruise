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
package edu.snu.cay.common.aggregation;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Register and unregister driver and evaluators to/from Network Connection Service.
 */
public final class AggregationNetworkSetup {
  private static final String AGGREGATION_IDENTIFIER = "AGGREGATION";

  private final NetworkConnectionService networkConnectionService;
  private final Identifier connectionFactoryIdentifier;
  private final AggregationMsgCodec codec;
  private final AggregationMsgHandler handler;
  private ConnectionFactory connectionFactory;

  @Inject
  private AggregationNetworkSetup(final NetworkConnectionService networkConnectionService,
                                  final IdentifierFactory identifierFactory,
                                  final AggregationMsgCodec codec,
                                  final AggregationMsgHandler handler) throws NetworkException {
    this.networkConnectionService = networkConnectionService;
    this.connectionFactoryIdentifier = identifierFactory.getNewInstance(AGGREGATION_IDENTIFIER);
    this.codec = codec;
    this.handler = handler;
  }

  public ConnectionFactory registerConnectionFactory(final Identifier localEndPointId) {
    connectionFactory = networkConnectionService.registerConnectionFactory(connectionFactoryIdentifier,
        codec, handler, null, localEndPointId);
    return connectionFactory;
  }

  public void unregisterConnectionFactory() {
    networkConnectionService.unregisterConnectionFactory(connectionFactoryIdentifier);
  }

  public ConnectionFactory getConnectionFactory() {
    if (connectionFactory == null) {
      throw new RuntimeException("A connection factory has not been registered yet.");
    }
    return connectionFactory;
  }

  public Identifier getMyId() {
    if (connectionFactory == null) {
      throw new RuntimeException("A connection factory has not been registered yet.");
    }
    return connectionFactory.getLocalEndPointId();
  }
}
