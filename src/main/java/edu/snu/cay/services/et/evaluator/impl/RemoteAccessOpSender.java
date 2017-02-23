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
import edu.snu.cay.services.et.evaluator.api.TableComponents;
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
   * @param key a data key
   * @param value an Optional with a data value
   * @param targetEvalId a target evaluator
   * @param <V> a type of data
   * @return an operation holding the result
   */
  <K, V> RemoteDataOp<K, V> sendOpToRemote(final OpType opType,
                                           final String tableId, final int blockId,
                                           final K key, @Nullable final V value,
                                           final String targetEvalId) {
    final long operationId = remoteOpIdCounter.getAndIncrement();
    final String origId = executorId;
    final RemoteDataOp<K, V> operation = new RemoteDataOp<>(origId, operationId, opType,
        tableId, blockId, key, value);
    final DataOpMetadata<K, V> opMetadata = operation.getMetadata();

    LOG.log(Level.FINEST, "Send op to remote. OpId: {0}, OpType: {1}, targetId: {2}",
        new Object[]{opMetadata.getOpId(), opMetadata.getOpType(), targetEvalId});

    registerOp(operation);

    try {
      final TableComponents<K, V> tableComponents = tablesFuture.get().get(tableId);

      final KVSerializer<K, V> kvSerializer = tableComponents.getSerializer();
      final Codec<K> keyCodec = kvSerializer.getKeyCodec();
      final Codec<V> valueCodec = kvSerializer.getValueCodec();

      // encode data
      final ByteBuffer encodedKey = ByteBuffer.wrap(keyCodec.encode(opMetadata.getKey()));
      final DataValue dataValue;
      if (opMetadata.getOpType().equals(OpType.PUT) || opMetadata.getOpType().equals(OpType.UPDATE)) {
        if (!opMetadata.getValue().isPresent()) {
          throw new RuntimeException("Data value is empty for PUT/UPDATE");
        }
        final ByteBuffer encodedValue = ByteBuffer.wrap(valueCodec.encode(opMetadata.getValue().get()));
        dataValue = new DataValue(encodedValue);
      } else {
        dataValue = null;
      }

      msgSenderFuture.get().sendTableAccessReqMsg(origId, targetEvalId, opMetadata.getOpId(),
          tableId, opType, new DataKey(encodedKey), dataValue);

      try {
        if (!operation.waitRemoteOp(TIMEOUT_MS)) {
          LOG.log(Level.SEVERE, "Operation timeout. OpId: {0}", opMetadata.getOpId());
        } else {
          LOG.log(Level.FINE, "Operation successfully finished. OpId: {0}", opMetadata.getOpId());
        }
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Interrupted while waiting for executing remote operation", e);
      } finally {
        deregisterOp(opMetadata.getOpId());
      }
      // TODO #421: handle failures of operation (timeout, failed to locate).

      return operation;

    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Handles the result of remote operation.
   */
  <K, V> void onTableAccessResMsg(final long opId, final TableAccessResMsg msg) {
    final DataValue remoteOutput = msg.getDataValue();
    final boolean isSuccess = msg.getIsSuccess();

    @SuppressWarnings("unchecked")
    final RemoteDataOp<K, V> operation = ongoingOp.get(opId);
    if (operation == null) {
      LOG.log(Level.WARNING, "The operation is already handled or cancelled due to timeout. OpId: {0}", opId);
      return;
    }

    try {
      final TableComponents<K, V> tableComponents = tablesFuture.get().get(operation.getMetadata().getTableId());
      final KVSerializer<K, V> kvSerializer = tableComponents.getSerializer();
      final Codec<V> valueCodec = kvSerializer.getValueCodec();

      // decode data value
      final V decodedValue = isSuccess && remoteOutput != null ?
          valueCodec.decode(remoteOutput.getValue().array()) : null;

      operation.commitResult(decodedValue, isSuccess);

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
