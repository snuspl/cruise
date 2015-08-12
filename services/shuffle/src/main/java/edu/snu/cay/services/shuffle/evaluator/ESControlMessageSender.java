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

import edu.snu.cay.services.shuffle.driver.ShuffleDriverConfiguration;
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
 */
@EvaluatorSide
public final class ESControlMessageSender {

  private Connection<ShuffleControlMessage> connectionToManager;
  private final String shuffleName;

  @Inject
  private ESControlMessageSender(
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      @Parameter(ShuffleParameters.ShuffleName.class) final String shuffleName,
      final ShuffleNetworkSetup shuffleNetworkSetup) {
    this.shuffleName = shuffleName;
    final ConnectionFactory<ShuffleControlMessage> connFactory = shuffleNetworkSetup.getControlConnectionFactory();
    connectionToManager = connFactory.newConnection(idFactory
        .getNewInstance(ShuffleDriverConfiguration.SHUFFLE_DRIVER_NETWORK_IDENTIFIER));
    try {
      connectionToManager.open();
    } catch (final NetworkException e) {
      throw new RuntimeException("An NetworkException occurred while opening a connection to driver.", e);
    }
  }

  /**
   * Send a ShuffleControlMessage with code to the manager.
   *
   * @param code a control message code
   */
  public void sendToManager(final int code) {
    connectionToManager.write(new ShuffleControlMessage(code, shuffleName));
  }

  /**
   * Send a ShuffleControlMessage with code and dataList to the manager.
   *
   * @param code a control message code
   * @param dataList a data list
   */
  public void sendToManager(final int code, final List<byte[]> dataList) {
    connectionToManager.write(new ShuffleControlMessage(code, shuffleName, dataList));
  }
}
