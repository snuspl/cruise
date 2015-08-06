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
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.List;

/**
 * Driver-side control message sender.
 */
@DriverSide
public final class ControlMessageSender {

  private final String shuffleName;
  private final IdentifierFactory idFactory;
  private final ConnectionFactory<ShuffleControlMessage> connectionFactory;

  @Inject
  private ControlMessageSender(
      final ShuffleDescription shuffleDescription,
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      final NetworkConnectionService networkConnectionService) {
    this.shuffleName = shuffleDescription.getShuffleName();
    this.idFactory = idFactory;
    final Identifier controlMessageNetworkId = idFactory.getNewInstance(
        ShuffleParameters.SHUFFLE_CONTROL_MSG_NETWORK_ID);
    connectionFactory = networkConnectionService.getConnectionFactory(controlMessageNetworkId);
  }

  /**
   * Send a ShuffleControlMessage with code to endPointId.
   * It throws a NetworkException when an error occurs while opening a connection. In this case,
   * the link listener does not call any callbacks as the message is not tried to be sent.
   *
   * @param endPointId an end point id
   * @param code a control message code
   * @throws NetworkException
   */
  public void send(final String endPointId, final int code) throws NetworkException {
    send(endPointId, code, null);
  }

  /**
   * Send a ShuffleControlMessage with code and dataList to endPointId.
   * It throws a NetworkException when an error occurs while opening a connection. In this case,
   * the link listener does not call any callbacks as the message is not tried to be sent.
   *
   * @param endPointId an end point id
   * @param code a control message code
   * @param dataList a data list
   * @throws NetworkException
   */
  public void send(final String endPointId, final int code, final List<byte[]> dataList) throws NetworkException {
    final Connection<ShuffleControlMessage> connection = connectionFactory.newConnection(
        idFactory.getNewInstance(endPointId));
    connection.open();
    connection.write(new ShuffleControlMessage(code, shuffleName, dataList));
  }
}
