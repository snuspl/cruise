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
package edu.snu.cay.services.aggregate;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

public final class AggregationSlave<T> {

  private final AggregateNetworkSetup<T> aggregateNetworkSetup;
  private final Identifier masterId;
  private Connection<T> connection;

  @Inject
  private AggregationSlave(final AggregateNetworkSetup<T> aggregateNetworkSetup,
                           @Parameter(MasterId.class) final String masterIdStr,
                           final IdentifierFactory identifierFactory) {
    this.aggregateNetworkSetup = aggregateNetworkSetup;
    this.masterId = identifierFactory.getNewInstance(masterIdStr);

  }

  public void send(final T data) {
    if (connection == null) {
      connection = aggregateNetworkSetup.getConnectionFactory().newConnection(masterId);
      try {
        connection.open();
      } catch (final NetworkException e) {
        throw new RuntimeException("NetworkException during AggregationSlave.send()", e);
      }
    }

    connection.write(data);
  }
}
