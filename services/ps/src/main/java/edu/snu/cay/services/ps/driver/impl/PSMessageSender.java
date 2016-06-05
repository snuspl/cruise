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
package edu.snu.cay.services.ps.driver.impl;

import edu.snu.cay.services.ps.avro.AvroPSMsg;
import edu.snu.cay.services.ps.ns.PSNetworkSetup;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Sends messages from the Driver to ParameterWorkers.
 * Only used in the DynamicParameterServer implementation
 * to send the routing information acquired from Elastic Memory.
 */
// TODO #553: Should be instantiated only when DynamicParameterServer is used.
@DriverSide
final class PSMessageSender {
  private final InjectionFuture<PSNetworkSetup> psNetworkSetup;
  private final IdentifierFactory identifierFactory;

  @Inject
  private PSMessageSender(final InjectionFuture<PSNetworkSetup> psNetworkSetup,
                          final IdentifierFactory identifierFactory) {
    this.psNetworkSetup = psNetworkSetup;
    this.identifierFactory = identifierFactory;
  }

  void send(final String destId, final AvroPSMsg msg) {
    final ConnectionFactory<AvroPSMsg> connFactory = psNetworkSetup.get().getConnectionFactory();
    if (connFactory == null) {
      throw new RuntimeException("ConnectionFactory has not been registered or has been removed");
    }

    final Connection<AvroPSMsg> conn = connFactory
        .newConnection(identifierFactory.getNewInstance(destId));
    try {
      conn.open();
      conn.write(msg);
    } catch (final NetworkException ex) {
      throw new RuntimeException("NetworkException during connection open/write", ex);
    }
  }
}
