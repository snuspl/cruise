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
package edu.snu.cay.services.em.evaluator;

import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * A class that sends data operations to corresponding remote evaluators.
 */
public final class OperationRemoteSender {
  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

  private OperationResultHandler resultHandler;

  private final Serializer serializer;

  @Inject
  private OperationRemoteSender(final InjectionFuture<ElasticMemoryMsgSender> msgSender,
                                final OperationResultHandler resultHandler,
                                final Serializer serializer) {
    this.msgSender = msgSender;
    this.resultHandler = resultHandler;
    this.serializer = serializer;
  }

  /**
   * Send a data operation to a target remote evaluator.
   */
  public void sendOperation(final String targetEvalId, final DataOperation operation) {
    final Codec codec = serializer.getCodec(operation.getDataType());

    // client thread goes to sleep until the operation is completed
    operation.setRemote();

    // register only local requests
    if (operation.getOrigEvalId() == null) {
      resultHandler.registerOperation(operation);
    }

    try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_OP")) {
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());

      final ByteBuffer inputData = operation.getOperationType() == DataOpType.PUT ?
          ByteBuffer.wrap(codec.encode(operation.getDataValue())) : null;

      msgSender.get().sendRemoteOpMsg(operation.getOrigEvalId(), targetEvalId, operation.getOperationType(),
          operation.getDataType(), operation.getDataKey(), inputData, operation.getOperationId(), traceInfo);
    }
  }
}
