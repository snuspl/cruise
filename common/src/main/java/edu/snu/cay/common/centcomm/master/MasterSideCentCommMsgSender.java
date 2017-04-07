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
package edu.snu.cay.common.centcomm.master;

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.ns.CentCommNetworkSetup;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Master of CentComm Service.
 * Sends messages to CentComm slaves.
 */
@DriverSide
public final class MasterSideCentCommMsgSender {

  /**
   * A network setup instance for CentCommMsg. It should be wrapped with InjectionFuture
   * since there would be a loopy constructor when users use MasterSideCentCommMsgSender in their
   * CentComm message handlers.
   */
  private final InjectionFuture<CentCommNetworkSetup> centCommNetworkSetup;

  /**
   * An identifier factory.
   */
  private final IdentifierFactory identifierFactory;

  @Inject
  private MasterSideCentCommMsgSender(final InjectionFuture<CentCommNetworkSetup> centCommNetworkSetup,
                                      final IdentifierFactory identifierFactory) {
    this.centCommNetworkSetup = centCommNetworkSetup;
    this.identifierFactory = identifierFactory;
  }

  /**
   * Sends a message to a CentComm slave named slaveId. The user should specify
   * class name of the CentComm service client.
   *
   * @param clientClassName class name of the CentComm service client
   * @param slaveId an end point id of the slave
   * @param data data which is encoded as a byte array
   * @throws NetworkException when target slave has been unregistered
   */
  public void send(final String clientClassName, final String slaveId, final byte[] data) throws NetworkException {
    final CentCommMsg msg = CentCommMsg.newBuilder()
        .setSourceId(centCommNetworkSetup.get().getMyId().toString())
        .setClientClassName(clientClassName)
        .setData(ByteBuffer.wrap(data))
        .build();
    final Connection<CentCommMsg> conn = centCommNetworkSetup.get().getConnectionFactory()
        .newConnection(identifierFactory.getNewInstance(slaveId));
    conn.open();
    conn.write(msg);
  }
}
