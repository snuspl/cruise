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

import edu.snu.cay.services.ps.avro.AvroPSMsg;
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

  private final PSMsgCodec psMsgCodec;
  private final EventHandler<Message<AvroPSMsg>> handler;
  private volatile ConnectionFactory<AvroPSMsg> connectionFactory;

  @Inject
  private PSNetworkSetup(
      final NetworkConnectionService networkConnectionService,
      final IdentifierFactory identifierFactory,
      final PSMsgCodec psMsgCodec,
      @Parameter(PSMessageHandler.class) final EventHandler<Message<AvroPSMsg>> handler)
      throws NetworkException {
    this.networkConnectionService = networkConnectionService;
    this.connectionFactoryIdentifier = identifierFactory.getNewInstance(PARAMETER_SERVER_IDENTIFIER);
    this.psMsgCodec = psMsgCodec;
    this.handler = handler;
  }

  /**
   * Registers a PS worker into {@link NetworkConnectionService}.
   * It registers the PS worker with id, {@code localEndPointId} into a name server of NCS,
   * and obtains a {@link ConnectionFactory} creating connections to Server.
   * After registration, {@link #getConnectionFactory()} and {@link #getMyId()} methods will get valid return values.
   * @param localEndPointId a local id that will represent the PS client in NCS
   * @return a ConnectionFactory for creating connections to Server.
   */
  public ConnectionFactory<AvroPSMsg> registerConnectionFactory(final Identifier localEndPointId) {
    connectionFactory = networkConnectionService.registerConnectionFactory(connectionFactoryIdentifier,
        psMsgCodec, handler, null, localEndPointId);
    return connectionFactory;
  }

  /**
   * Unregisters a PS worker from {@link NetworkConnectionService}.
   * The {@link #connectionFactory} field becomes null as before registration.
   */
  public void unregisterConnectionFactory() {
    connectionFactory = null;
    networkConnectionService.unregisterConnectionFactory(connectionFactoryIdentifier);
  }

  /**
   * Returns a ConnectionFactory for creating connections to Server.
   * It returns a valid value only when the PS worker is registered to NCS,
   * otherwise returns null.
   * @return a ConnectionFactory when it is registered to NCS, otherwise returns null
   */
  public ConnectionFactory<AvroPSMsg> getConnectionFactory() {
    return connectionFactory;
  }

  /**
   * Returns a ConnectionFactory for creating connections to Server.
   * It returns a valid value only when the PS worker is registered to NCS,
   * otherwise returns null.
   * @return an Identifier when it is registered to NCS, otherwise returns null
   */
  public Identifier getMyId() {
    if (connectionFactory == null) {
      return null;
    }
    return connectionFactory.getLocalEndPointId();
  }
}
