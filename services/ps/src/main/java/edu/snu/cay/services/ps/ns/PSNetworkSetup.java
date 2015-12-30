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
package edu.snu.cay.services.ps.ns;

import edu.snu.cay.services.ps.avro.AvroParameterServerMsg;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Register and unregister an evaluator to/from Network Connection Service, and open connections to other evaluators.
 */
public final class PSNetworkSetup {
  private static final String PARAMETER_SERVER_IDENTIFIER = "PS";

  private final NetworkConnectionService networkConnectionService;
  private final Identifier connectionFactoryIdentifier;
  private final ParameterServerMsgCodec parameterServerMsgCodec;
  private final EventHandler<Message<AvroParameterServerMsg>> handler;
  private ConnectionFactory<AvroParameterServerMsg> connectionFactory;

  @Inject
  private PSNetworkSetup(
      final NetworkConnectionService networkConnectionService,
      final IdentifierFactory identifierFactory,
      final ParameterServerMsgCodec parameterServerMsgCodec,
      @Parameter(PSMessageHandler.class) final EventHandler<Message<AvroParameterServerMsg>> handler)
      throws NetworkException {
    this.networkConnectionService = networkConnectionService;
    this.connectionFactoryIdentifier = identifierFactory.getNewInstance(PARAMETER_SERVER_IDENTIFIER);
    this.parameterServerMsgCodec = parameterServerMsgCodec;
    this.handler = handler;
  }

  public ConnectionFactory<AvroParameterServerMsg> registerConnectionFactory(final Identifier localEndPointId) {
    connectionFactory = networkConnectionService.registerConnectionFactory(connectionFactoryIdentifier,
        parameterServerMsgCodec, handler, null, localEndPointId);
    return connectionFactory;
  }

  public void unregisterConnectionFactory() {
    networkConnectionService.unregisterConnectionFactory(connectionFactoryIdentifier);
  }

  public ConnectionFactory<AvroParameterServerMsg> getConnectionFactory() {
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
