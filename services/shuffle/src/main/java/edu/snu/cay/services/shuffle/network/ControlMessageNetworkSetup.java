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
package edu.snu.cay.services.shuffle.network;

import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;

/**
 * Setup a connection factory of ShuffleControlMessage.
 */
public final class ControlMessageNetworkSetup implements AutoCloseable {

  private static final String SHUFFLE_CONTROL_CONNECTION_FACTORY_ID_PREFIX = "SHUFFLE_CONTROL_";

  private final NetworkConnectionService networkConnectionService;
  private final Identifier controlConnectionFactoryId;
  private final ShuffleControlMessageHandler controlMessageHandler;
  private final ShuffleControlLinkListener controlLinkListener;
  private final ConnectionFactory<ShuffleControlMessage> connectionFactory;

  @Inject
  private ControlMessageNetworkSetup(
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      @Parameter(ShuffleParameters.ShuffleName.class) final String shuffleName,
      @Parameter(ShuffleParameters.EndPointId.class) final String endPointId,
      final NetworkConnectionService networkConnectionService,
      final ShuffleControlMessageHandler controlMessageHandler,
      final ShuffleControlLinkListener controlLinkListener,
      final ShuffleControlMessageCodec controlMessageCodec) {
    this.networkConnectionService = networkConnectionService;
    this.controlMessageHandler = controlMessageHandler;
    this.controlLinkListener = controlLinkListener;
    this.controlConnectionFactoryId = idFactory
        .getNewInstance(getControlConnectionFactoryId(shuffleName));

    final Identifier endPointIdentifier = idFactory.getNewInstance(endPointId);
    this.connectionFactory = networkConnectionService.registerConnectionFactory(
        controlConnectionFactoryId,
        controlMessageCodec,
        controlMessageHandler,
        controlLinkListener,
        endPointIdentifier
    );
  }

  private String getControlConnectionFactoryId(final String shuffleName) {
    return SHUFFLE_CONTROL_CONNECTION_FACTORY_ID_PREFIX + shuffleName;
  }

  /**
   * Set a message handler and a link listener for ShuffleControlMessage.
   *
   * @param messageHandler a message handler of ShuffleControlMessage
   * @param linkListener a link listener of ShuffleControlMessage
   */
  public void setControlMessageHandlerAndLinkListener(
      final EventHandler<Message<ShuffleControlMessage>> messageHandler,
      final LinkListener<Message<ShuffleControlMessage>> linkListener) {
    controlMessageHandler.setControlMessageHandler(messageHandler);
    controlLinkListener.setControlLinkListener(linkListener);
  }

  /**
   * @return the connection factory of ShuffleControlMessage
   */
  public ConnectionFactory<ShuffleControlMessage> getControlConnectionFactory() {
    return connectionFactory;
  }

  @Override
  public void close() {
    networkConnectionService.unregisterConnectionFactory(controlConnectionFactoryId);
  }
}
