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

import edu.snu.cay.services.ps.ParameterServerParameters.KeyCodecName;
import edu.snu.cay.services.ps.ParameterServerParameters.PreValueCodecName;
import edu.snu.cay.services.ps.avro.AvroParameterServerMsg;
import edu.snu.cay.services.ps.avro.PullMsg;
import edu.snu.cay.services.ps.avro.PushMsg;
import edu.snu.cay.services.ps.server.api.ParameterServer;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.hadoop.util.hash.MurmurHash;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * Server-side Parameter Server message handler.
 * Decode messages and call the appropriate {@link ParameterServer} method.
 * We also compute a {@link MurmurHash} on the encoded key and pass it to {@link ParameterServer}.
 *
 * An alternative approach would be to compute the hash at the client and send it as part of the message.
 * This would trade-off less computation on the server for more computation on the client and more communication cost.
 */
@EvaluatorSide
public final class ServerSideMsgHandler<K, P, V> implements EventHandler<Message<AvroParameterServerMsg>> {
  private static final Logger LOG = Logger.getLogger(ServerSideMsgHandler.class.getName());

  /**
   * The Parameter Server object.
   */
  private final ParameterServer<K, P, V> parameterServer;

  /**
   * Codec for decoding PS keys.
   */
  private final Codec<K> keyCodec;

  /**
   * Codec for decoding PS preValues.
   */
  private final Codec<P> preValueCodec;

  @Inject
  private ServerSideMsgHandler(final ParameterServer<K, P, V> parameterServer,
                               @Parameter(KeyCodecName.class) final Codec<K> keyCodec,
                               @Parameter(PreValueCodecName.class) final Codec<P> preValueCodec) {
    this.parameterServer = parameterServer;
    this.keyCodec = keyCodec;
    this.preValueCodec = preValueCodec;
  }

  /**
   * Hand over values given from workers to {@link ParameterServer}.
   * Throws an exception if messages of an unexpected type arrive.
   */
  @Override
  public void onNext(final Message<AvroParameterServerMsg> msg) {
    LOG.entering(ServerSideMsgHandler.class.getSimpleName(), "onNext");

    final AvroParameterServerMsg innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case PushMsg:
      onPushMsg(innerMsg.getPushMsg());
      break;

    case PullMsg:
      onPullMsg(innerMsg.getPullMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message type: " + innerMsg.getType().toString());
    }

    LOG.exiting(ServerSideMsgHandler.class.getSimpleName(), "onNext");
  }

  private void onPushMsg(final PushMsg pushMsg) {
    final K key = keyCodec.decode(pushMsg.getKey().array());
    final P preValue = preValueCodec.decode(pushMsg.getPreValue().array());
    final int keyHash = hash(pushMsg.getKey().array());
    parameterServer.push(key, preValue, keyHash);
  }

  private void onPullMsg(final PullMsg pullMsg) {
    final String srcId = pullMsg.getSrcId().toString();
    final K key = keyCodec.decode(pullMsg.getKey().array());
    final int keyHash = hash(pullMsg.getKey().array());
    parameterServer.pull(key, srcId, keyHash);
  }

  private int hash(final byte[] encodedKey) {
    return Math.abs(MurmurHash.getInstance().hash(encodedKey));
  }
}
