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
package edu.snu.cay.services.ps.worker.partitioned;

import edu.snu.cay.services.ps.ParameterServerParameters.PreValueCodecName;
import edu.snu.cay.services.ps.avro.*;
import edu.snu.cay.services.ps.ns.PSNetworkSetup;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * A Msg Sender for PartitionedWorker.
 */
@EvaluatorSide
public final class PartitionedWorkerMsgSender<K, P> {

  /**
   * Network Connection Service related setup required for a Parameter Server application.
   */
  private final PSNetworkSetup psNetworkSetup;

  /**
   * Required for using Network Connection Service API.
   */
  private final IdentifierFactory identifierFactory;

  /**
   * Codec for encoding PS preValues.
   */
  private final Codec<P> preValueCodec;

  private final String driverIdentifier;

  @Inject
  private PartitionedWorkerMsgSender(final PSNetworkSetup psNetworkSetup,
                                     final IdentifierFactory identifierFactory,
                                     @Parameter(PreValueCodecName.class) final Codec<P> preValueCodec,
                                     @Parameter(DriverIdentifier.class) final String driverIdentifier) {
    this.psNetworkSetup = psNetworkSetup;
    this.identifierFactory = identifierFactory;
    this.preValueCodec = preValueCodec;
    this.driverIdentifier = driverIdentifier;
  }

  private void send(final String destId, final AvroParameterServerMsg msg) {
    final Connection<AvroParameterServerMsg> conn = psNetworkSetup.getConnectionFactory()
        .newConnection(identifierFactory.getNewInstance(destId));
    try {
      conn.open();
      conn.write(msg);
    } catch (final NetworkException ex) {
      throw new RuntimeException("NetworkException during connection open/write", ex);
    }
  }

  /**
   * Sends a push msg for {@code key} to a corresponding server.
   * @param destId an id of destination server
   * @param key a key to push
   * @param preValue a previous value to push
   */
  void sendPushMsg(final String destId, final EncodedKey<K> key, final P preValue) {
    final PushMsg pushMsg = PushMsg.newBuilder()
        .setKey(ByteBuffer.wrap(key.getEncoded()))
        .setPreValue(ByteBuffer.wrap(preValueCodec.encode(preValue)))
        .build();

    send(destId,
        AvroParameterServerMsg.newBuilder()
            .setType(Type.PushMsg)
            .setPushMsg(pushMsg)
            .build());
  }

  /**
   * Sends a pull msg for {@code key} to a corresponding server.
   * @param destId an id of destination server
   * @param key a key to pull
   */
  void sendPullMsg(final String destId, final EncodedKey<K> key) {
    final PullMsg pullMsg = PullMsg.newBuilder()
        .setKey(ByteBuffer.wrap(key.getEncoded()))
        .setSrcId(psNetworkSetup.getMyId().toString())
        .build();

    send(destId,
        AvroParameterServerMsg.newBuilder()
            .setType(Type.PullMsg)
            .setPullMsg(pullMsg)
            .build());
  }

  /**
   * Sends a msg to register itself to driver for subscribing the up-to-date routing table
   * to resolve PS server when performing push/pull operations.
   * At first, the worker will receive the whole routing table and
   * will be kept being updated with the further changes in the routing table by driver.
   * This method is valid only for Dynamic PS.
   */
  public void sendWorkerRegisterMsg() {
    final WorkerRegisterMsg workerRegisterMsg = WorkerRegisterMsg.newBuilder()
        .build();
    send(driverIdentifier,
        AvroParameterServerMsg.newBuilder()
            .setType(Type.WorkerRegisterMsg)
            .setWorkerRegisterMsg(workerRegisterMsg)
            .build());
  }

  /**
   * Sends a msg to deregister itself, then driver stops feeding updates in the routing table.
   * This method is valid only for Dynamic PS.
   */
  public void sendWorkerDeregisterMsg() {
    final WorkerDeregisterMsg workerDeregisterMsg = WorkerDeregisterMsg.newBuilder()
        .build();
    send(driverIdentifier,
        AvroParameterServerMsg.newBuilder()
            .setType(Type.WorkerDeregisterMsg)
            .setWorkerDeregisterMsg(workerDeregisterMsg)
            .build());
  }
}
