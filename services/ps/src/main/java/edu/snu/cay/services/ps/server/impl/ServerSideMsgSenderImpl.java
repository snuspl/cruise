/*
 * Copyright (C) 2015 Seoul National University
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

import edu.snu.cay.services.ps.ParameterServerParameters.KeyCodecName;
import edu.snu.cay.services.ps.ParameterServerParameters.ValueCodecName;
import edu.snu.cay.services.ps.avro.AvroParameterServerMsg;
import edu.snu.cay.services.ps.avro.ReplyMsg;
import edu.snu.cay.services.ps.avro.Type;
import edu.snu.cay.services.ps.ns.PSNetworkSetup;
import edu.snu.cay.services.ps.server.ValueEntry;
import edu.snu.cay.services.ps.server.api.ServerSideMsgSender;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Default implementation of {@link ServerSideMsgSender}.
 */
@EvaluatorSide
public final class ServerSideMsgSenderImpl<K, V> implements ServerSideMsgSender<K, V> {

  /**
   * Network Connection Service related setup required for a Parameter Server application.
   */
  private final PSNetworkSetup psNetworkSetup;

  /**
   * Required for using Network Connection Service API.
   */
  private final IdentifierFactory identifierFactory;

  /**
   * Codec for encoding PS keys.
   */
  private final Codec<K> keyCodec;

  /**
   * Codec for encoding PS values.
   */
  private final Codec<V> valueCodec;


  @Inject
  private ServerSideMsgSenderImpl(final PSNetworkSetup psNetworkSetup,
                                  final IdentifierFactory identifierFactory,
                                  @Parameter(KeyCodecName.class) final Codec<K> keyCodec,
                                  @Parameter(ValueCodecName.class) final Codec<V> valueCodec) {
    this.psNetworkSetup = psNetworkSetup;
    this.identifierFactory = identifierFactory;
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
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

  @Override
  public void sendReplyMsg(final String destId, final K key, final ValueEntry<V> valueEntry) {
    final ByteBuffer valueByteArray;

    // The updater may be writing a new value right now, so we acquire a read lock before proceeding
    valueEntry.getReadWriteLock().readLock().lock();
    try {
      valueByteArray = ByteBuffer.wrap(valueCodec.encode(valueEntry.getValue()));
    } finally {
      valueEntry.getReadWriteLock().readLock().unlock();
    }

    final ReplyMsg replyMsg = ReplyMsg.newBuilder()
        .setKey(ByteBuffer.wrap(keyCodec.encode(key)))
        .setValue(valueByteArray)
        .build();

    send(destId,
        AvroParameterServerMsg.newBuilder()
            .setType(Type.ReplyMsg)
            .setReplyMsg(replyMsg)
            .build());
  }
}
