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
package edu.snu.cay.common.aggregation.slave;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.ns.AggregationNetworkSetup;
import edu.snu.cay.common.aggregation.ns.MasterId;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Slave of Aggregation Service.
 * Sends messages to aggregation master.
 */
@EvaluatorSide
public final class AggregationSlave {

  private final AggregationNetworkSetup aggregationNetworkSetup;
  private final Identifier masterId;

  @Inject
  private AggregationSlave(final AggregationNetworkSetup aggregationNetworkSetup,
                           @Parameter(MasterId.class) final String masterIdStr,
                           final IdentifierFactory identifierFactory) {
    this.aggregationNetworkSetup = aggregationNetworkSetup;
    this.masterId = identifierFactory.getNewInstance(masterIdStr);
  }

  /**
   * Sends message to aggregation master. The user should specify class name of the aggregation service client.
   * @param clientClassName class name of the aggregation service client
   * @param data data which is encoded as a byte array
   */
  public void send(final String clientClassName, final byte[] data) {
    final AggregationMessage msg = AggregationMessage.newBuilder()
        .setSlaveId(aggregationNetworkSetup.getMyId().toString())
        .setClientClassName(clientClassName)
        .setData(ByteBuffer.wrap(data))
        .build();
    final Connection<AggregationMessage> conn = aggregationNetworkSetup.getConnectionFactory().newConnection(masterId);
    try {
      conn.open();
      conn.write(msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException during AggregationSlave.send()", e);
    }
  }
}
