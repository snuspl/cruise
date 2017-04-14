/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.avro.*;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that 1) sends remote access request msgs to a remote executor that owns data block
 * and 2) handles the result received from the executor.
 */
final class RemoteAccessOpSender {
  private static final Logger LOG = Logger.getLogger(RemoteAccessOpSender.class.getName());

  /**
   * A counter for issuing ids for operations sent to remote executors.
   */
  private final AtomicLong opIdCounter = new AtomicLong(0);

  /**
   * A map holding ongoing operations until they finish.
   * It only maintains operations requested from local clients.
   */
  private final ConcurrentMap<Long, RemoteDataOp> ongoingOp = new ConcurrentHashMap<>();

  private final InjectionFuture<Tables> tablesFuture;
  private final String executorId;
  private final InjectionFuture<MessageSender> msgSenderFuture;
  private final InjectionFuture<RemoteAccessOpStat> networkUsageStatFuture;

  @Inject
  private RemoteAccessOpSender(final InjectionFuture<Tables> tablesFuture,
                               @Parameter(ExecutorIdentifier.class) final String executorId,
                               final InjectionFuture<MessageSender> msgSenderFuture,
                               final InjectionFuture<RemoteAccessOpStat> networkUsageStatFuture) {
    this.tablesFuture = tablesFuture;
    this.executorId = executorId;
    this.msgSenderFuture = msgSenderFuture;
    this.networkUsageStatFuture = networkUsageStatFuture;
  }

  /**
   * Send operation to remote evaluators.
   * @param opType a type of operation
   * @param tableId a table id
   * @param blockId an identifier of block, to which the data key belongs
   * @param key a data key
   * @param value a data value, which can be null
   * @param updateValue an update date value, which can be null
   * @param targetEvalId a target evaluator
   * @param replyRequired a boolean representing that the operation requires reply or not
   * @param <K> a type of key
   * @param <V> a type of value
   * @param <U> a type of update value
   * @return an operation holding the result
   */
  <K, V, U> DataOpResult<V> sendOpToRemote(final OpType opType,
                                           final String tableId, final int blockId,
                                           final K key,
                                           @Nullable final V value,
                                           @Nullable final U updateValue,
                                           final String targetEvalId,
                                           final boolean replyRequired) {
    final long operationId = opIdCounter.getAndIncrement();
    final RemoteDataOp<K, V, U> operation = new RemoteDataOp<>(executorId, operationId, opType, replyRequired,
        tableId, blockId, key, value, updateValue);
    final DataOpMetadata<K, V, U> opMetadata = operation.getMetadata();

    LOG.log(Level.FINEST, "Send op to remote. OpId: {0}, OpType: {1}, targetId: {2}",
        new Object[]{opMetadata.getOpId(), opMetadata.getOpType(), targetEvalId});

    if (replyRequired) {
      registerOp(operation);
    }

    encodeAndSendRequestMsg(opMetadata, targetEvalId);

    return operation.getDataOpResult();
  }

  private <K, V, U> void encodeAndSendRequestMsg(final DataOpMetadata<K, V, U> opMetadata,
                                                 final String targetEvalId) {
    try {
      final TableComponents<K, V, U> tableComponents = tablesFuture.get().getTableComponents(opMetadata.getTableId());
      final KVUSerializer<K, V, U> kvuSerializer = tableComponents.getSerializer();
      final Codec<K> keyCodec = kvuSerializer.getKeyCodec();
      final Codec<V> valueCodec = kvuSerializer.getValueCodec();
      final Codec<U> updateValueCodec = kvuSerializer.getUpdateValueCodec();

      // encode data
      final ByteBuffer encodedKey = ByteBuffer.wrap(keyCodec.encode(opMetadata.getKey()));

      final DataValue dataValue;
      if (opMetadata.getOpType().equals(OpType.PUT) || opMetadata.getOpType().equals(OpType.PUT_IF_ABSENT)) {
        if (!opMetadata.getValue().isPresent()) {
          throw new RuntimeException("Data value is empty for PUT");
        }
        final ByteBuffer encodedValue = ByteBuffer.wrap(
            valueCodec.encode(opMetadata.getValue().get()));
        dataValue = new DataValue(encodedValue);
      } else if (opMetadata.getOpType().equals(OpType.UPDATE)) {
        if (!opMetadata.getUpdateValue().isPresent()) {
          throw new RuntimeException("Data value is empty for PUT");
        }
        final ByteBuffer encodedUpdateValue = ByteBuffer.wrap(
            updateValueCodec.encode(opMetadata.getUpdateValue().get()));
        // treat UpdateValue same as Value
        dataValue = new DataValue(encodedUpdateValue);
      } else  {
        dataValue = null;
      }
      
      // TODO #106: Collect metrics about all remote access operations
      if (opMetadata.getOpType().equals(OpType.GET)) {
        networkUsageStatFuture.get().incCountSentGetReq(opMetadata.getTableId());
      }

      msgSenderFuture.get().sendTableAccessReqMsg(opMetadata.getOrigId(), targetEvalId, opMetadata.getOpId(),
          opMetadata.getTableId(), opMetadata.getOpType(), opMetadata.isReplyRequired(),
          new DataKey(encodedKey), dataValue);

    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Handles the result of remote operation.
   */
  @SuppressWarnings("unchecked")
  <V> void onTableAccessResMsg(final long opId, final TableAccessResMsg msg) {
    final DataValue remoteOutput = msg.getDataValue();
    final boolean isSuccess = msg.getIsSuccess();

    final RemoteDataOp<?, V, ?> operation = ongoingOp.get(opId);
    if (operation == null) {
      LOG.log(Level.WARNING, "The operation is already handled or cancelled due to timeout. OpId: {0}", opId);
      return;
    }

    final String tableId = operation.getMetadata().getTableId();
    try {
      final Codec<V> valueCodec = (Codec<V>) tablesFuture.get().getTableComponents(tableId)
          .getSerializer().getValueCodec();

      // decode data value
      final V decodedValue = isSuccess && remoteOutput != null ?
          valueCodec.decode(remoteOutput.getValue().array()) : null;

      // TODO #106: Collect metrics about all remote access operations
      if (operation.getMetadata().getOpType().equals(OpType.GET)
          && remoteOutput != null && remoteOutput.getValue() != null) {
        networkUsageStatFuture.get().incBytesReceivedGetResp(tableId, remoteOutput.getValue().array().length);
      }

      operation.getDataOpResult().commitResult(decodedValue, isSuccess);

      deregisterOp(opId);
      LOG.log(Level.FINEST, "Remote operation is finished. OpId: {0}", opId);
    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Registers an operation before sending it to remote executor.
   */
  private void registerOp(final RemoteDataOp operation) {
    final RemoteDataOp unhandledOperation = ongoingOp.put(operation.getMetadata().getOpId(), operation);
    if (unhandledOperation != null) {
      LOG.log(Level.SEVERE, "Discard the exceptionally unhandled operation: {0}",
          unhandledOperation.getMetadata().getOpId());
    }
  }

  /**
   * Deregisters an operation after its remote access is finished.
   */
  private void deregisterOp(final long operationId) {
    ongoingOp.remove(operationId);
  }
}
