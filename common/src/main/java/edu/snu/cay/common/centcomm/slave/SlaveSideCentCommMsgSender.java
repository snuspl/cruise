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
package edu.snu.cay.common.centcomm.slave;

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.ns.CentCommNetworkSetup;
import edu.snu.cay.common.centcomm.ns.MasterId;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Slave of CentComm Service.
 * Sends messages to CentComm comm.
 */
@EvaluatorSide
public final class SlaveSideCentCommMsgSender {

  /**
   * A network setup instance for CentCommMsg. It should be wrapped with InjectionFuture
   * since there would be a loopy constructor when users use SlaveSideCentCommMsgSender in their
   * CentComm message handlers.
   */
  private final InjectionFuture<CentCommNetworkSetup> centCommNetworkSetup;

  /**
   * An identifier of the comm.
   */
  private final Identifier masterId;

  @Inject
  private SlaveSideCentCommMsgSender(final InjectionFuture<CentCommNetworkSetup> centCommNetworkSetup,
                                     @Parameter(MasterId.class) final String masterIdStr,
                                     final IdentifierFactory identifierFactory) {
    this.centCommNetworkSetup = centCommNetworkSetup;
    this.masterId = identifierFactory.getNewInstance(masterIdStr);
  }

  /**
   * Sends message to CentComm comm. The user should specify class name of the CentComm service client.
   * @param clientClassName class name of the CentComm service client
   * @param data data which is encoded as a byte array
   */
  public void send(final String clientClassName, final byte[] data) {
    final CentCommMsg msg = CentCommMsg.newBuilder()
        .setSourceId(centCommNetworkSetup.get().getMyId().toString())
        .setClientClassName(clientClassName)
        .setData(ByteBuffer.wrap(data))
        .build();
    final Connection<CentCommMsg> conn = centCommNetworkSetup.get().
        getConnectionFactory().newConnection(masterId);
    try {
      conn.open();
      conn.write(msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException during SlaveSideCentCommMsgSender.send()", e);
    }
  }
}
