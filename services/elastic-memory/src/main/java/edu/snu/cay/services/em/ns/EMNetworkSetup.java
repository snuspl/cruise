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
package edu.snu.cay.services.em.ns;

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.ns.parameters.EMCodec;
import edu.snu.cay.services.em.ns.parameters.EMMessageHandler;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

public final class EMNetworkSetup {
  private static final String EM_IDENTIFIER = "EM";

  private final NetworkConnectionService networkConnectionService;
  private final Identifier connectionFactoryIdentifier;
  private final Codec<AvroElasticMemoryMessage> codec;
  private final EventHandler<Message<AvroElasticMemoryMessage>> handler;
  private ConnectionFactory<AvroElasticMemoryMessage> connectionFactory;

  @Inject
  private EMNetworkSetup(
      final NetworkConnectionService networkConnectionService,
      final IdentifierFactory identifierFactory,
      @Parameter(EMCodec.class) final Codec<AvroElasticMemoryMessage> codec,
      @Parameter(EMMessageHandler.class) final EventHandler<Message<AvroElasticMemoryMessage>> handler
  ) throws NetworkException {
    this.networkConnectionService = networkConnectionService;
    this.connectionFactoryIdentifier = identifierFactory.getNewInstance(EM_IDENTIFIER);
    this.codec = codec;
    this.handler = handler;
  }

  public ConnectionFactory<AvroElasticMemoryMessage> registerConnectionFactory(final Identifier localEndPointId) {
    connectionFactory = networkConnectionService.registerConnectionFactory(connectionFactoryIdentifier,
        codec, handler, null, localEndPointId);
    return connectionFactory;
  }

  public void unregisterConnectionFactory() {
    networkConnectionService.unregisterConnectionFactory(connectionFactoryIdentifier);
  }

  public ConnectionFactory<AvroElasticMemoryMessage> getConnectionFactory() {
    return connectionFactory;
  }

  public Identifier getMyId() {
    return connectionFactory.getLocalEndPointId();
  }
}
