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
package edu.snu.cay.services.ps.server.impl;

import edu.snu.cay.services.ps.PSParameters;
import edu.snu.cay.services.ps.avro.*;
import edu.snu.cay.services.ps.ns.PSNetworkSetup;
import edu.snu.cay.services.ps.server.api.ServerSideReplySender;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Sender implementation that uses Network Connection Service.
 */
@EvaluatorSide
public final class ServerSideReplySenderImpl<K, P, V> implements ServerSideReplySender<K, P, V> {

  /**
   * Network Connection Service related setup required for a Parameter Server application.
   */
  private final InjectionFuture<PSNetworkSetup> psNetworkSetup;

  /**
   * Required for using Network Connection Service API.
   */
  private final IdentifierFactory identifierFactory;

  /**
   * Codec for encoding PS keys.
   */
  private final Codec<K> keyCodec;

  /**
   * Codec for encoding PS preValues.
   */
  private final Codec<P> preValueCodec;

  /**
   * Codec for encoding PS values.
   */
  private final Codec<V> valueCodec;

  @Inject
  private ServerSideReplySenderImpl(
      final InjectionFuture<PSNetworkSetup> psNetworkSetup,
      final IdentifierFactory identifierFactory,
      @Parameter(PSParameters.KeyCodecName.class) final Codec<K> keyCodec,
      @Parameter(PSParameters.PreValueCodecName.class) final Codec<P> preValueCodec,
      @Parameter(PSParameters.ValueCodecName.class) final Codec<V> valueCodec) {

    this.psNetworkSetup = psNetworkSetup;
    this.identifierFactory = identifierFactory;
    this.keyCodec = keyCodec;
    this.preValueCodec = preValueCodec;
    this.valueCodec = valueCodec;
  }

  private void send(final String destId, final AvroPSMsg msg) {
    final ConnectionFactory<AvroPSMsg> connFactory = psNetworkSetup.get().getConnectionFactory();
    if (connFactory == null) {
      throw new RuntimeException("ConnectionFactory has not been registered, or has been removed accidentally");
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

  /**
   * Send an Avro message via NetworkConnectionService.
   */
  @Override
  public void sendPullReplyMsg(final String destId, final K key, final V value, final long processingTime) {
    final PullReplyMsg pullReplyMsg = PullReplyMsg.newBuilder()
        .setKey(ByteBuffer.wrap(keyCodec.encode(key)))
        .setValue(ByteBuffer.wrap(valueCodec.encode(value)))
        .setServerProcessingTime(processingTime)
        .build();

    send(destId,
        AvroPSMsg.newBuilder()
            .setType(Type.PullReplyMsg)
            .setPullReplyMsg(pullReplyMsg)
            .build());
  }

  @Override
  public void sendPushRejectMsg(final String destId, final K key, final P preValue) {
    final PushRejectMsg pushRejectMsg = PushRejectMsg.newBuilder()
        .setKey(ByteBuffer.wrap(keyCodec.encode(key)))
        .setPreValue(ByteBuffer.wrap(preValueCodec.encode(preValue)))
        .build();

    send(destId,
        AvroPSMsg.newBuilder()
            .setType(Type.PushRejectMsg)
            .setPushRejectMsg(pushRejectMsg)
            .build());
  }

  @Override
  public void sendPullRejectMsg(final String destId, final K key) {
    final PullRejectMsg pullRejectMsg = PullRejectMsg.newBuilder()
        .setKey(ByteBuffer.wrap(keyCodec.encode(key)))
        .build();

    send(destId,
        AvroPSMsg.newBuilder()
            .setType(Type.PullRejectMsg)
            .setPullRejectMsg(pullRejectMsg)
            .build());
  }
}
