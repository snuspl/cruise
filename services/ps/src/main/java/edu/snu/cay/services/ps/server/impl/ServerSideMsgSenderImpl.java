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
import edu.snu.cay.services.ps.server.api.ServerSideMsgSender;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;
import org.htrace.Span;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Sender implementation that uses Network Connection Service.
 */
@EvaluatorSide
public final class ServerSideMsgSenderImpl<K, P, V> implements ServerSideMsgSender<K, P, V> {

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
  private ServerSideMsgSenderImpl(
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
  public void sendPullReplyMsg(final String destId, final K key, final V value,
                               final int requestId, final long processingTime, @Nullable final TraceInfo traceInfo) {
    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;
    try (final TraceScope sendPullReplyScope = Trace.startSpan(
        String.format("send_pull_reply. key: %s, request_id: %d", key, requestId), traceInfo)) {
      final PullReplyMsg pullReplyMsg = PullReplyMsg.newBuilder()
          .setKey(ByteBuffer.wrap(keyCodec.encode(key)))
          .setValue(ByteBuffer.wrap(valueCodec.encode(value)))
          .setRequestId(requestId)
          .setServerProcessingTime(processingTime)
          .build();

      detached = sendPullReplyScope.detach();

      send(destId,
          AvroPSMsg.newBuilder()
              .setType(Type.PullReplyMsg)
              .setPullReplyMsg(pullReplyMsg)
              .setTraceInfo(HTraceUtils.toAvro(TraceInfo.fromSpan(detached)))
              .build());
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  @Override
  public void sendPushMsg(final String destId, final K key, final P preValue) {
    final byte[] serializedKey = keyCodec.encode(key);
    final byte[] serializedPreValue = preValueCodec.encode(preValue);
    final PushMsg pushMsg = PushMsg.newBuilder()
        .setKey(ByteBuffer.wrap(serializedKey))
        .setPreValue(ByteBuffer.wrap(serializedPreValue))
        .build();

    send(destId,
        AvroPSMsg.newBuilder()
            .setType(Type.PushMsg)
            .setPushMsg(pushMsg)
            .build());
  }

  @Override
  public void sendPullMsg(final String destId, final String requesterId, final K key, final int requestId,
                          @Nullable final TraceInfo traceInfo) {
    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    try (final TraceScope sendPushMsgScope = Trace.startSpan(
        String.format("redirect_pull_msg. key: %s, req_id: %d, server_id: %s", key, requestId, destId), traceInfo)) {

      detached = sendPushMsgScope.detach();

      final byte[] serializedKey = keyCodec.encode(key);
      final PullMsg pullMsg = PullMsg.newBuilder()
          .setRequesterId(requesterId)
          .setKey(ByteBuffer.wrap(serializedKey))
          .setRequestId(requestId)
          .build();

      send(destId,
          AvroPSMsg.newBuilder()
              .setType(Type.PullMsg)
              .setPullMsg(pullMsg)
              .setTraceInfo(HTraceUtils.toAvro(TraceInfo.fromSpan(detached)))
              .build());
    } finally {
      Trace.continueSpan(detached).close();
    }
  }
}
