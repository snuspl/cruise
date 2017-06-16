/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.dolphin.async.network;

import edu.snu.cay.dolphin.async.DolphinMsg;
import edu.snu.cay.services.et.exceptions.AlreadyConnectedException;
import edu.snu.cay.services.et.exceptions.NotConnectedException;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
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
public final class NetworkConnectionImpl implements NetworkConnection<DolphinMsg> {
  private static final Logger LOG = Logger.getLogger(NetworkConnectionImpl.class.getName());

  private final NetworkConnectionService networkConnectionService;
  private final Codec<DolphinMsg> codec;
  private final MessageHandler msgHandler;
  private final NetworkLinkListener networkLinkListener;
  private final IdentifierFactory identifierFactory;
  private final String jobId;

  /**
   * Member variables for holding network connection instance.
   */
  private ConnectionFactory<DolphinMsg> connectionFactory;

  @Inject
  private NetworkConnectionImpl(final NetworkConnectionService networkConnectionService,
                                final IdentifierFactory identifierFactory,
                                @Parameter(JobIdentifier.class) final String jobId,
                                final DolphinMsgCodec codec,
                                final MessageHandler msgHandler,
                                final NetworkLinkListener networkLinkListener) {
    this.networkConnectionService = networkConnectionService;
    this.codec = codec;
    this.msgHandler = msgHandler;
    this.networkLinkListener = networkLinkListener;
    this.identifierFactory = identifierFactory;
    this.jobId = jobId;
  }

  @Override
  public void setup(final String endPointId) {
    if (connectionFactory != null) {
      throw new AlreadyConnectedException(connectionFactory.getConnectionFactoryId(),
          connectionFactory.getLocalEndPointId());
    }

    final Identifier connectionFactoryId = identifierFactory.getNewInstance(jobId);
    final Identifier localEndPointId = identifierFactory.getNewInstance(endPointId);
    connectionFactory = networkConnectionService.registerConnectionFactory(connectionFactoryId, codec,
        msgHandler, networkLinkListener, localEndPointId);
    LOG.log(Level.INFO, "Established network connection {0}/{1}",
        new Object[]{connectionFactoryId, localEndPointId});
  }

  @Override
  public void send(final String destId, final DolphinMsg msg) throws NotConnectedException, NetworkException {
    if (connectionFactory == null) {
      throw new NotConnectedException();
    }
    final Connection<DolphinMsg> connection = connectionFactory.newConnection(
        identifierFactory.getNewInstance(destId));
    connection.open();
    connection.write(msg);
  }
}
