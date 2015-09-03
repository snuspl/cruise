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
package edu.snu.cay.services.shuffle.driver;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.network.*;
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
 * Driver-side network setup.
 *
 * Users should set a control message handler and a control link listener to create a connection factory
 * of ShuffleControlMessage.
 */
public final class DSNetworkSetup implements AutoCloseable {

  private final NetworkConnectionService networkConnectionService;
  private final Identifier controlConnectionFactoryId;
  private final Identifier driverLocalEndPointId;
  private final ShuffleControlMessageCodec controlMessageCodec;

  @Inject
  private DSNetworkSetup(
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      final ShuffleDescription shuffleDescription,
      final NetworkConnectionService networkConnectionService,
      final ShuffleControlMessageCodec controlMessageCodec) {
    final String shuffleName = shuffleDescription.getShuffleName();
    this.networkConnectionService = networkConnectionService;
    this.controlConnectionFactoryId = idFactory
        .getNewInstance(ShuffleParameters.getControlConnectionFactoryId(shuffleName));
    this.driverLocalEndPointId = idFactory
        .getNewInstance(ShuffleParameters.SHUFFLE_DRIVER_LOCAL_END_POINT_ID);
    this.controlMessageCodec = controlMessageCodec;
  }

  /**
   * @return the connection factory for ShuffleControlMessage
   */
  public ConnectionFactory<ShuffleControlMessage> getControlConnectionFactory() {
    return networkConnectionService.getConnectionFactory(controlConnectionFactoryId);
  }

  /**
   * Register a connection factory of ShuffleControlMessage with controlMessageHandler and controlLinkListener.
   *
   * @param controlMessageHandler a message handler of ShuffleControlMessage
   * @param controlLinkListener a link listener of ShuffleControlMessage
   */
  public void setControlMessageHandlerAndLinkListener(
      final EventHandler<Message<ShuffleControlMessage>> controlMessageHandler,
      final LinkListener<Message<ShuffleControlMessage>> controlLinkListener) {
    networkConnectionService.registerConnectionFactory(
        controlConnectionFactoryId,
        controlMessageCodec,
        controlMessageHandler,
        controlLinkListener,
        driverLocalEndPointId
    );
  }

  /**
   * Unregister connection factories.
   */
  @Override
  public void close() {
    networkConnectionService.unregisterConnectionFactory(controlConnectionFactoryId);
  }
}
