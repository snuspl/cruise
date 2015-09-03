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
package edu.snu.cay.services.shuffle.evaluator;

import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.List;

/**
 * Evaluator-side control message sender.
 *
 * The connection factory of ShuffleControlMessage should be registered first through ESNetworkSetup.
 *
 * Note that Shuffle, ShuffleSender, ShuffleReceiver can not send control messages
 * through this class in the constructor of them.
 */
@EvaluatorSide
public final class ESControlMessageSender {

  private final IdentifierFactory idFactory;
  private final ESNetworkSetup networkSetup;
  private ConnectionFactory<ShuffleControlMessage> connectionFactory;
  private Connection<ShuffleControlMessage> connectionToManager;

  /**
   * Construct a evaluator-side control message sender.
   * This should be instantiated once for each shuffle, using several forked injectors.
   *
   * @param idFactory an identifier factory
   * @param networkSetup a network setup
   */
  @Inject
  private ESControlMessageSender(
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      final ESNetworkSetup networkSetup) {
    this.idFactory = idFactory;
    this.networkSetup = networkSetup;
  }

  private Connection<ShuffleControlMessage> getConnectionToManager() {
    if (connectionToManager == null) {
      connectionToManager = getConnectionFactory()
          .newConnection(idFactory.getNewInstance(ShuffleParameters.SHUFFLE_DRIVER_LOCAL_END_POINT_ID));
      try {
        connectionToManager.open();
      } catch (final NetworkException e) {
        // TODO #67: failure handling.
        throw new RuntimeException("Failed to open a connection to the driver");
      }
    }

    return connectionToManager;
  }

  private ConnectionFactory<ShuffleControlMessage> getConnectionFactory() {
    if (connectionFactory == null) {
      connectionFactory = networkSetup.getControlConnectionFactory();
    }

    return connectionFactory;
  }

  /**
   * Send a ShuffleControlMessage with code to the manager.
   *
   * @param code a control message code
   */
  public void sendToManager(final int code) {
    getConnectionToManager().write(new ShuffleControlMessage(code));
  }

  /**
   * Send a ShuffleControlMessage with code and endPointIdList to the manager.
   *
   * @param code a control message code
   * @param endPointIdList a list of end point ids
   */
  public void sendToManager(final int code, final List<String> endPointIdList) {
    getConnectionToManager().write(new ShuffleControlMessage(code, endPointIdList));
  }

  /**
   * Send a ShuffleControlMessage with code to the endPointId.
   *
   * @param endPointId an end point id where the message will be sent
   * @param code a control message code
   */
  public void sendTo(final String endPointId, final int code) {
    sendTo(endPointId, new ShuffleControlMessage(code));
  }

  /**
   * Send a ShuffleControlMessage with code and endPointIdList to the endPointId.
   *
   * @param endPointId an end point id where the message will be sent
   * @param code a control message code
   * @param endPointIdList a list of end point ids
   */
  public void sendTo(final String endPointId, final int code, final List<String> endPointIdList) {
    sendTo(endPointId, new ShuffleControlMessage(code, endPointIdList));
  }

  private void sendTo(final String endPointId, final ShuffleControlMessage controlMessage) {
    final Connection<ShuffleControlMessage> connection = getConnectionFactory().newConnection(
        idFactory.getNewInstance(endPointId));
    try {
      connection.open();
      connection.write(controlMessage);
    } catch (final NetworkException ex) {
      // TODO #67: failure handling.
      throw new RuntimeException("An NetworkException occurred while sending message to" + endPointId, ex);
    }
  }
}
