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
package edu.snu.cay.services.ps.worker;

import edu.snu.cay.services.ps.ParameterServerParameters.KeyCodecName;
import edu.snu.cay.services.ps.ParameterServerParameters.ValueCodecName;
import edu.snu.cay.services.ps.avro.AvroParameterServerMsg;
import edu.snu.cay.services.ps.avro.ReplyMsg;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * Worker-side Parameter Server message handler.
 */
@EvaluatorSide
public final class WorkerSideMsgHandler<K, P, V> implements EventHandler<Message<AvroParameterServerMsg>> {
  private static final Logger LOG = Logger.getLogger(WorkerSideMsgHandler.class.getName());

  /**
   * This evaluator's worker that is expecting Parameter Server messages.
   */
  private final ParameterWorker<K, P, V> parameterWorker;

  /**
   * Codec for decoding PS keys.
   */
  private final Codec<K> keyCodec;

  /**
   * Codec for decoding PS values.
   */
  private final Codec<V> valueCodec;

  @Inject
  private WorkerSideMsgHandler(final ParameterWorker<K, P, V> parameterWorker,
                               @Parameter(KeyCodecName.class) final Codec<K> keyCodec,
                               @Parameter(ValueCodecName.class) final Codec<V> valueCodec) {
    this.parameterWorker = parameterWorker;
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
  }

  /**
   * Hand over values given from the server to {@link ParameterWorker}.
   * Throws an exception if messages of an unexpected type arrive.
   */
  @Override 
  public void onNext(final Message<AvroParameterServerMsg> msg) {
    LOG.entering(WorkerSideMsgHandler.class.getSimpleName(), "onNext");

    final AvroParameterServerMsg innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case ReplyMsg:
      onReplyMsg(innerMsg.getReplyMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message type: " + innerMsg.getType().toString());
    }

    LOG.exiting(WorkerSideMsgHandler.class.getSimpleName(), "onNext");
  }

  private void onReplyMsg(final ReplyMsg replyMsg) {
    final K key = keyCodec.decode(replyMsg.getKey().array());
    final V value = valueCodec.decode(replyMsg.getValue().array());
    parameterWorker.processReply(key, value);
  }
}
