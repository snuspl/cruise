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
package edu.snu.cay.services.et.common.impl;

import edu.snu.cay.services.et.common.api.NetworkConnection;
import edu.snu.cay.services.et.configuration.parameters.ETIdentifier;
import edu.snu.cay.services.et.exceptions.AlreadyConnectedException;
import edu.snu.cay.services.et.exceptions.NotConnectedException;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.util.StringCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation for {@link NetworkConnection}.
 */
public final class NetworkConnectionImpl implements NetworkConnection {
  private static final Logger LOG = Logger.getLogger(NetworkConnectionImpl.class.getName());

  private final NetworkConnectionService networkConnectionService;
  private final Codec<String> stringCodec;
  private final NetworkEventHandler networkEventHandler;
  private final NetworkLinkListener networkLinkListener;
  private final Identifier connectionFactoryId;

  /**
   * Member variables for holding network connection instance.
   */
  private ConnectionFactory<String> connectionFactory;
  private Identifier localEndPointId;

  @Inject
  private NetworkConnectionImpl(final NetworkConnectionService networkConnectionService,
                                final IdentifierFactory identifierFactory,
                                final StringCodec stringCodec,
                                final NetworkEventHandler networkEventHandler,
                                final NetworkLinkListener networkLinkListener,
                                @Parameter(ETIdentifier.class) final String elasticTableId) {
    this.networkConnectionService = networkConnectionService;
    this.stringCodec = stringCodec;
    this.networkEventHandler = networkEventHandler;
    this.networkLinkListener = networkLinkListener;
    this.connectionFactoryId = identifierFactory.getNewInstance(elasticTableId);
  }

  @Override
  public void setup(final Identifier endPointId) {
    if (connectionFactory != null) {
      throw new AlreadyConnectedException(connectionFactoryId, localEndPointId);
    }

    localEndPointId = endPointId;
    connectionFactory = networkConnectionService.registerConnectionFactory(connectionFactoryId, stringCodec,
        networkEventHandler, networkLinkListener, localEndPointId);
    LOG.log(Level.INFO, "Established network connection {0}/{1}", new Object[]{connectionFactoryId, localEndPointId});
  }

  @Override
  public void send(final Identifier destId, final String msg) throws NotConnectedException, NetworkException {
    if (connectionFactory == null) {
      throw new NotConnectedException();
    }

    final Connection<String> connection = connectionFactory.newConnection(destId);
    connection.open();
    connection.write(msg);
  }
}
