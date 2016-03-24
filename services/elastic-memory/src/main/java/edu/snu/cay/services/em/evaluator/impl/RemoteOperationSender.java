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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.util.Optional;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that sends data operations to corresponding remote evaluators.
 */
final class RemoteOperationSender {
  private static final Logger LOG = Logger.getLogger(RemoteOperationSender.class.getName());
  private static final long TIMEOUT_MS = 40000;

  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

  private OperationResultHandler resultHandler;

  private final Serializer serializer;

  @Inject
  private RemoteOperationSender(final InjectionFuture<ElasticMemoryMsgSender> msgSender,
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

    if (operation.isFromLocalClient()) {
      resultHandler.registerOperation(operation);
    }

    try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_OP")) {
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());

      final ByteBuffer inputData = operation.getInputData().isPresent() ?
          ByteBuffer.wrap(codec.encode(operation.getInputData().get())) : null;

      final Optional<String> origEvalID = operation.getOrigEvalId();

      msgSender.get().sendRemoteOpMsg(origEvalID.get(), targetEvalId, operation.getOperationType(),
          operation.getDataType(), operation.getDataKey(), inputData, operation.getOperationId(), traceInfo);
    }

    // local request threads wait here until get the result
    if (operation.isFromLocalClient()) {
      try {
        operation.waitOperation(TIMEOUT_MS);
      } catch (InterruptedException e) {
        LOG.warning("Thread is interrupted while waiting for executing remote operation");
      }

      if (resultHandler.deregisterOperation(operation.getOperationId()) != null) {
        LOG.log(Level.WARNING, "The operation {0} has no response in timeout.", operation.getOperationId());
      }
    }
  }
}
