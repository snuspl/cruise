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
package edu.snu.cay.services.em.evaluator.impl.singlekey;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.KeyCodecName;
import edu.snu.cay.services.em.evaluator.api.SingleKeyOperation;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that executes all jobs related to remote access.
 * It 1) sends operation to remote stores and 2) sends the result of remote operation to the origin store,
 * and 3) receives and handles the received result.
 */
public final class RemoteOpHandler<K> implements EventHandler<AvroElasticMemoryMessage> {
  private static final Logger LOG = Logger.getLogger(RemoteOpHandler.class.getName());
  private static final long TIMEOUT_MS = 40000;

  /**
   * A map holding ongoing operations until they finish.
   * It only maintains operations requested from local clients.
   */
  private final ConcurrentMap<String, SingleKeyOperation<K, Object>> ongoingOp = new ConcurrentHashMap<>();

  private Serializer serializer;
  private Codec<K> keyCodec;
  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

  @Inject
  private RemoteOpHandler(final Serializer serializer,
                          @Parameter(KeyCodecName.class) final Codec<K> keyCodec,
                          final InjectionFuture<ElasticMemoryMsgSender> msgSender) {
    this.serializer = serializer;
    this.keyCodec = keyCodec;
    this.msgSender = msgSender;
  }

  /**
   * Send operation to remote evaluators.
   * @param operation an operation
   * @param <V> a type of data
   */
  <V> void sendOpToRemoteStore(final SingleKeyOperation<K, V> operation, final String targetEvalId) {

    LOG.log(Level.FINEST, "Send op to remote. OpId: {0}, OpType: {1}",
        new Object[]{operation.getOpId(), operation.getOpType()});

    registerOp(operation);

    final Codec<V> dataCodec = serializer.getCodec(operation.getDataType());

    try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_OP")) {
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());

      // encode data
      final ByteBuffer encodedKey = ByteBuffer.wrap(keyCodec.encode(operation.getKey()));
      final DataValue dataValue;
      if (operation.getOpType() == DataOpType.PUT) {
        if (!operation.getValue().isPresent()) {
          throw new RuntimeException("Data value is empty for PUT");
        }
        final ByteBuffer encodedData = ByteBuffer.wrap(dataCodec.encode(operation.getValue().get()));
        dataValue = new DataValue(encodedData);
      } else {
        dataValue = null;
      }

      msgSender.get().sendRemoteOpMsg(operation.getOrigEvalId().get(), targetEvalId,
          operation.getOpType(), operation.getDataType(), new DataKey(encodedKey), dataValue,
          operation.getOpId(), traceInfo);
    }

    try {
      if (!operation.waitRemoteOps(TIMEOUT_MS)) {
        LOG.log(Level.SEVERE, "Operation timeout. OpId: {0}", operation.getOpId());
      } else {
        LOG.log(Level.FINE, "Operation successfully finished. OpId: {0}", operation.getOpId());
      }
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Interrupted while waiting for executing remote operation", e);
    } finally {
      deregisterOp(operation.getOpId());
    }
    // TODO #421: handle failures of operation (timeout, failed to locate).
  }

  /**
   * Handles the result of remote operation.
   */
  @Override
  public void onNext(final AvroElasticMemoryMessage msg) {

    final RemoteOpResultMsg remoteOpResultMsg = msg.getRemoteOpResultMsg();
    final String operationId = msg.getOperationId().toString();
    final DataValue remoteOutput = (DataValue) remoteOpResultMsg.getDataValues();
    final boolean isSuccess = remoteOpResultMsg.getIsSuccess();

    final SingleKeyOperation<K, Object> operation = ongoingOp.get(operationId);

    if (operation == null) {
      LOG.log(Level.WARNING, "The operation is already handled or cancelled due to timeout. OpId: {0}", operationId);
      return;
    }

    // decode data value
    final Optional<Object> decodedValue;
    if (isSuccess && remoteOutput != null) {
      final Codec dataCodec = serializer.getCodec(operation.getDataType());
      decodedValue = Optional.of(dataCodec.decode(remoteOutput.getValue().array()));
    } else {
      decodedValue = Optional.empty();
    }

    operation.commitResult(decodedValue, isSuccess);

    LOG.log(Level.FINEST, "Remote operation succeed. OpId: {0}", operationId);
  }

  /**
   * Registers an operation before sending it to remote memory store.
   */
  private void registerOp(final SingleKeyOperation operation) {
    final SingleKeyOperation unhandledOperation = ongoingOp.put(operation.getOpId(), operation);
    if (unhandledOperation != null) {
      LOG.log(Level.SEVERE, "Discard the exceptionally unhandled operation: {0}",
          unhandledOperation.getOpId());
    }
  }

  /**
   * Deregisters an operation after its remote access is finished.
   */
  private void deregisterOp(final String operationId) {
    ongoingOp.remove(operationId);
  }

  /**
   * Sends the result to the original store.
   */
  <V> void sendResultToOrigin(final SingleKeyOperation<K, V> operation) {

    LOG.log(Level.FINEST, "Send result to origin. OpId: {0}, OrigId: {1}",
        new Object[]{operation.getOpId(), operation.getOrigEvalId()});

    // send the original store the result (RemoteOpResultMsg)
    try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_RESULT")) {
      final String dataType = operation.getDataType();
      final Codec<Object> dataCodec = serializer.getCodec(dataType);

      final Optional<String> origEvalId = operation.getOrigEvalId();

      final DataValue dataValue;
      if (operation.getOutputData().isPresent()) {
        final V outputData = operation.getOutputData().get();
        dataValue = new DataValue(ByteBuffer.wrap(dataCodec.encode(outputData)));
      } else {
        dataValue = null;
      }

      msgSender.get().sendRemoteOpResultMsg(origEvalId.get(), dataValue, true,
          operation.getOpId(), TraceInfo.fromSpan(traceScope.getSpan()));
    }
  }
}
