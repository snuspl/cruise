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
package edu.snu.cay.common.aggregation;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
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

  public void send(final String clientId, final byte[] data) {
    final AggregationMessage msg = AggregationMessage.newBuilder()
        .setSrcId(aggregationNetworkSetup.getMyId().toString())
        .setClientId(clientId)
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
