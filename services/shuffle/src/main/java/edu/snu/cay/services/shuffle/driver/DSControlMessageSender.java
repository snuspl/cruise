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

import edu.snu.cay.services.shuffle.network.ControlMessageNetworkSetup;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Driver-side control message sender.
 *
 * Note that a control message handler and a control link listener should be set first through
 * ControlMessageNetworkSetup to use this class.
 */
@DriverSide
public final class DSControlMessageSender {

  private final IdentifierFactory idFactory;
  private final ConnectionFactory<ShuffleControlMessage> connectionFactory;
  private final Map<String, Connection<ShuffleControlMessage>> connectionMap;

  /**
   * Construct a driver-side control message sender.
   * This should be instantiated once for each shuffle manager, using several forked injectors.
   *
   * @param idFactory an identifier factory
   * @param controlMessageNetworkSetup a control message network setup
   */
  @Inject
  private DSControlMessageSender(
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      final ControlMessageNetworkSetup controlMessageNetworkSetup) {
    this.idFactory = idFactory;
    this.connectionFactory = controlMessageNetworkSetup.getControlConnectionFactory();
    this.connectionMap = new HashMap<>();
  }

  /**
   * Send a ShuffleControlMessage with code to the endPointId
   * It throws a NetworkException when an error occurs while opening a connection. In this case,
   * the link listener does not call any callbacks as the message is not tried to be sent.
   *
   * @param endPointId an end point id
   * @param code a control message code
   * @throws NetworkException
   */
  public void send(final String endPointId, final int code) throws NetworkException {
    send(endPointId, new ShuffleControlMessage(code));
  }

  /**
   * Send a ShuffleControlMessage with code and endPointIdList to the endPointId
   * It throws a NetworkException when an error occurs while opening a connection. In this case,
   * the link listener does not call any callbacks as the message is not tried to be sent.
   *
   * @param endPointId an end point id
   * @param code a control message code
   * @param endPointIdList a list of end point ids
   * @throws NetworkException
   */
  public void send(final String endPointId, final int code, final List<String> endPointIdList) throws NetworkException {
    send(endPointId, new ShuffleControlMessage(code, endPointIdList));
  }

  /**
   * Send a ShuffleControlMessage to the endPointId
   * It throws a NetworkException when an error occurs while opening a connection. In this case,
   * the link listener does not call any callbacks as the message is not tried to be sent.
   *
   * @param endPointId an end point id
   * @param controlMessage a ShuffleControlMessage
   * @throws NetworkException
   */
  private void send(final String endPointId, final ShuffleControlMessage controlMessage) throws NetworkException {
    final Connection<ShuffleControlMessage> connection = getConnection(endPointId);
    connection.open();
    connection.write(controlMessage);
  }

  private Connection<ShuffleControlMessage> getConnection(final String endPointId) {
    synchronized (connectionMap) {
      if (!connectionMap.containsKey(endPointId)) {
        connectionMap.put(endPointId, connectionFactory.newConnection(idFactory.getNewInstance(endPointId)));
      }

      return connectionMap.get(endPointId);
    }
  }
}
