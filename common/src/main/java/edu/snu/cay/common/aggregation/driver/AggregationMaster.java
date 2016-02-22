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
package edu.snu.cay.common.aggregation.driver;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.ns.AggregationNetworkSetup;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Master of Aggregation Service.
 * Sends messages to aggregation slaves.
 */
@DriverSide
public final class AggregationMaster {

  private final InjectionFuture<AggregationNetworkSetup> aggregationNetworkSetup;
  private final IdentifierFactory identifierFactory;

  @Inject
  private AggregationMaster(final InjectionFuture<AggregationNetworkSetup> aggregationNetworkSetup,
                            final IdentifierFactory identifierFactory) {
    this.aggregationNetworkSetup = aggregationNetworkSetup;
    this.identifierFactory = identifierFactory;
  }

  /**
   * Sends a message to an aggregation slave named endPointId. The user should specify
   * class name of the aggregation service client.
   *
   * @param clientClassName class name of the aggregation service client
   * @param endPointId an end point id of the slave
   * @param data data which is encoded as a byte array
   */
  public void send(final String clientClassName, final String endPointId, final byte[] data) {
    final AggregationMessage msg = AggregationMessage.newBuilder()
        .setSourceId(aggregationNetworkSetup.get().getMyId().toString())
        .setClientClassName(clientClassName)
        .setData(ByteBuffer.wrap(data))
        .build();
    final Connection<AggregationMessage> conn = aggregationNetworkSetup.get().getConnectionFactory()
        .newConnection(identifierFactory.getNewInstance(endPointId));
    try {
      conn.open();
      conn.write(msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException during AggregationMaster.send()", e);
    }
  }
}
