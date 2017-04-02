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
import edu.snu.cay.services.et.evaluator.api.DataOpResult;
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
  private static final long TIMEOUT_MS = 40000;

  /**
   * A counter for issuing ids for operations sent to remote executors.
   */
  private final AtomicLong remoteOpIdCounter = new AtomicLong(0);

  /**
   * A map holding ongoing operations until they finish.
   * It only maintains operations requested from local clients.
   */
  private final ConcurrentMap<Long, RemoteDataOp> ongoingOp = new ConcurrentHashMap<>();

  private final InjectionFuture<Tables> tablesFuture;
  private final String executorId;
  private final InjectionFuture<MessageSender> msgSenderFuture;

  @Inject
  private RemoteAccessOpSender(final InjectionFuture<Tables> tablesFuture,
                               @Parameter(ExecutorIdentifier.class) final String executorId,
                               final InjectionFuture<MessageSender> msgSenderFuture) {
    this.tablesFuture = tablesFuture;
    this.executorId = executorId;
    this.msgSenderFuture = msgSenderFuture;
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
   * @param sync an boolean representing whether this method returns after waiting for the result or not
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
                                           final boolean sync) {
    final long operationId = remoteOpIdCounter.getAndIncrement();
    final String origId = executorId;
    final RemoteDataOp<K, V, U> operation = new RemoteDataOp<>(origId, operationId, opType,
        tableId, blockId, key, value, updateValue);
    final DataOpMetadata<K, V, U> opMetadata = operation.getMetadata();

    LOG.log(Level.FINEST, "Send op to remote. OpId: {0}, OpType: {1}, targetId: {2}",
        new Object[]{opMetadata.getOpId(), opMetadata.getOpType(), targetEvalId});

    registerOp(operation);

    try {
      final TableComponents<K, V, U> tableComponents = tablesFuture.get().getTableComponents(tableId);
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

      msgSenderFuture.get().sendTableAccessReqMsg(origId, targetEvalId, opMetadata.getOpId(),
          tableId, opType, new DataKey(encodedKey), dataValue);

      final DataOpResult<V> opResult = operation.getDataOpResult();
      if (sync) {
        // TODO #421: handle failures of operation (timeout, failed to locate).
        try {
          if (!opResult.waitRemoteOp(TIMEOUT_MS)) {
            LOG.log(Level.SEVERE, "Operation timeout. OpId: {0}", opMetadata.getOpId());
          } else {
            LOG.log(Level.FINE, "Operation successfully finished. OpId: {0}", opMetadata.getOpId());
          }
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Interrupted while waiting for executing remote operation", e);
        } finally {
          deregisterOp(opMetadata.getOpId());
        }
      }

      return opResult;
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
