/*
 *
 *  * Copyright (C) 2016 Seoul National University
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *         http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A class that handles the result of data operations both from local and remote memory stores.
 * The results are routed to a local client or a remote memory store where the operation is started.
 */
public final class OperationResultHandler {
  private static final Logger LOG = Logger.getLogger(OperationResultHandler.class.getName());

  private final Map<String, DataOperation> ongoingOperations = new HashMap<>();

  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

  private final Serializer serializer;

  @Inject
  OperationResultHandler(final Serializer serializer,
                         final InjectionFuture<ElasticMemoryMsgSender> msgSender) {
    this.serializer = serializer;
    this.msgSender = msgSender;
  }

  /**
   * Register an operation with ongoingOperations map.
   */
  public void registerOperation(final DataOperation operation) {
    ongoingOperations.put(operation.getOperationId(), operation);
  }

  /**
   * Deregister an operation from ongoingOperations map.
   */
  public void deregisterOperation(final String operationId) {
    ongoingOperations.remove(operationId);
  }

  /**
   * Handle the result of data operation that is processed by local memory store.
   * It returns the result to the local client or sends it to the remote evaluator
   * corresponding to the origin of the data operation.
   */
  public void handleLocalResult(final DataOperation operation, final boolean result, final Object outputData) {
    if (operation.isLocalRequest()) {
      // return the result to the local client
      operation.setResult(result, outputData);
    } else {
      // send the remote store the result (RemoteOpResultMsg)
      try (final TraceScope traceScope = Trace.startSpan("HANDLE_LOCAL_RESULT")) {
        final String dataType = operation.getDataType();
        final DataOpType opType = operation.getOperationType();

        final Codec codec = serializer.getCodec(dataType);
        final ByteBuffer data = opType == DataOpType.GET ? ByteBuffer.wrap(codec.encode(outputData)) : null;

        msgSender.get().sendRemoteOpResultMsg(operation.getOrigEvalId(), result, data,
            operation.getOperationId(), TraceInfo.fromSpan(traceScope.getSpan()));
      }
    }
  }

  /**
   * Handle the result of data operation that is processed by remote memory store.
   * It always return the result to the local client.
   */
  public void handleRemoteResult(final String operationId, final boolean result, final ByteBuffer data) {
    final DataOperation finishedOperation = ongoingOperations.remove(operationId);

    if (finishedOperation == null) {
      LOG.info("The operation is already handled or cancelled due to time out. OpId: " + operationId);
      return;
    }

    final Codec codec = serializer.getCodec(finishedOperation.getDataType());
    final Object outputData = data == null ? null : codec.decode(data.array());

    finishedOperation.setResultAndNotifyClientThread(result, outputData);
  }
}
