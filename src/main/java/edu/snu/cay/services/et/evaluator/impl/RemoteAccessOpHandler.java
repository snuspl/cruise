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
import edu.snu.cay.services.et.configuration.parameters.NumRemoteOpsHandlerThreads;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.evaluator.api.PartitionFunction;
import edu.snu.cay.services.et.evaluator.api.TableComponents;
import edu.snu.cay.services.et.exceptions.BlockNotExistsException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that sends and handles msgs about remote access operations.
 * It handles 1) remote operation msg that other executors requested to this executor,
 * 2) sends the result of the operation to the origin, and 3) handles the received result.
 */
public final class RemoteAccessOpHandler implements EventHandler<TableAccessMsg> {
  private static final Logger LOG = Logger.getLogger(RemoteAccessOpHandler.class.getName());
  private static final long TIMEOUT_MS = 40000;
  private static final int QUEUE_SIZE = 1024;
  private static final int QUEUE_TIMEOUT_MS = 3000;

  /**
   * A queue for operations requested from remote clients.
   */
  private final BlockingQueue<DataOpMetadata> operationQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);

  /**
   * A counter for issuing ids for operations sent to remote stores.
   */
  private final AtomicLong remoteOpIdCounter = new AtomicLong(0);

  /**
   * A map holding ongoing operations until they finish.
   * It only maintains operations requested from local clients.
   */
  private final ConcurrentMap<Long, RemoteDataOp> ongoingOp = new ConcurrentHashMap<>();

  private volatile boolean closeFlag = false;

  private final Tables tables;
  private final String executorId;
  private final InjectionFuture<MessageSender> msgSenderFuture;

  @Inject
  private RemoteAccessOpHandler(final Tables tables,
                                @Parameter(NumRemoteOpsHandlerThreads.class) final int numRemoteThreads,
                                @Parameter(ExecutorIdentifier.class) final String executorId,
                                final InjectionFuture<MessageSender> msgSenderFuture) {
    this.tables = tables;
    this.executorId = executorId;
    this.msgSenderFuture = msgSenderFuture;
    initExecutor(numRemoteThreads);
  }

  /**
   * Initialize threads that dequeue and execute operation from the {@code operationQueue}.
   * That is, these threads serve operations requested from remote clients.
   */
  private void initExecutor(final int numRemoteThreads) {
    final ExecutorService executor = Executors.newFixedThreadPool(numRemoteThreads);
    for (int i = 0; i < numRemoteThreads; i++) {
      executor.submit(new OperationThread());
    }
  }

  public void close() {
    closeFlag = true;
  }

  /**
   * A runnable that dequeues and executes operations requested from remote clients.
   * Several threads are initiated at the beginning and run as long-running background services.
   */
  private final class OperationThread implements Runnable {
    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      while (!closeFlag) {
        // First, poll and execute a single operation.
        // Poll with a timeout will prevent busy waiting, when the queue is empty.
        try {
          final DataOpMetadata operation =
              operationQueue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (operation == null) {
            continue;
          }

          handleOperation(operation);
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Poll failed with InterruptedException", e);
        }
      }
    }

    private <K, V> void handleOperation(final DataOpMetadata<K, V> operation) {
      final String tableId = operation.getTableId();
      final int blockId = operation.getBlockId();

      LOG.log(Level.FINEST, "Poll op: [OpId: {0}, origId: {1}, block: {2}]]",
          new Object[]{operation.getOpId(), operation.getOrigId(), blockId});

      try {
        final TableComponents<K, V> tableComponents = tables.get(tableId);
        final OwnershipCache ownershipCache = tableComponents.getOwnershipCache();
        final BlockStore<K, V> blockStore = tableComponents.getBlockStore();

        final Pair<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
        try {
          final Optional<String> remoteEvalIdOptional = remoteEvalIdWithLock.getKey();
          final boolean isLocal = !remoteEvalIdOptional.isPresent();
          if (isLocal) {
            final BlockImpl<K, V> block = (BlockImpl<K, V>) blockStore.get(blockId);

            final V output;
            boolean isSuccess = true;
            final OpType opType = operation.getOpType();
            switch (opType) {
            case PUT:
              output = block.put(operation.getKey(), operation.getValue().get());
              break;
            case GET:
              output = block.get(operation.getKey());
              break;
            case REMOVE:
              output = block.remove(operation.getKey());
              break;
            case UPDATE:
              output = block.update(operation.getKey(), operation.getValue().get());
              break;
            default:
              LOG.log(Level.WARNING, "Undefined type of operation.");
              output = null;
              isSuccess = false;
            }

            sendResultToOrigin(operation, output, isSuccess);
          } else {
            LOG.log(Level.WARNING,
                "Failed to execute operation {0} requested by remote store {2}." +
                    " This store was considered as the owner of block {1} by store {2}," +
                    " but the local ownership cache assumes store {3} is the owner",
                new Object[]{operation.getOpId(), blockId,
                    operation.getOrigId(), remoteEvalIdOptional.get()});

            // send the failed result
            sendResultToOrigin(operation, null, false);
          }
        } catch (final BlockNotExistsException e) {
          throw new RuntimeException(e);
        } finally {
          final Lock ownershipLock = remoteEvalIdWithLock.getValue();
          ownershipLock.unlock();
        }
      } catch (final TableNotExistException e) {
        throw new RuntimeException(e);
      }
    }
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
      final TableComponents<K, V> tableComponents = tables.get(tableId);

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
  @Override
  public void onNext(final TableAccessMsg msg) {
    switch (msg.getType()) {
    case TableAccessReqMsg:
      onTableAccessReqMsg(msg);
      break;

    case TableAccessResMsg:
      onTableAccessResMsg(msg);
      break;

    default:
      throw new RuntimeException("Illegal msg type: " + msg.getType());
    }
  }

  /**
   * Handles the data operation sent from the remote memory store.
   */
  private <K, V> void onTableAccessReqMsg(final TableAccessMsg msg) {
    final TableAccessReqMsg tableAccessReqMsg = msg.getTableAccessReqMsg();
    final String origEvalId = tableAccessReqMsg.getOrigId();
    final OpType opType = tableAccessReqMsg.getOpType();
    final String tableId = tableAccessReqMsg.getTableId();
    final DataKey dataKey = tableAccessReqMsg.getDataKey();
    final DataValue dataValue = tableAccessReqMsg.getDataValue();
    final long operationId = msg.getOperationId();

    try {
      final TableComponents<K, V> tableComponents = tables.get(tableId);
      final KVSerializer<K, V> kvSerializer = tableComponents.getSerializer();
      final Codec<K> keyCodec = kvSerializer.getKeyCodec();
      final Codec<V> valueCodec = kvSerializer.getValueCodec();
      final PartitionFunction<K> partitionFunction = tableComponents.getPartitionFunction();

      // decode data keys
      final K decodedKey = keyCodec.decode(dataKey.getKey().array());

      // decode data values
      final V decodedValue = opType.equals(OpType.PUT) || opType.equals(OpType.UPDATE) ?
          valueCodec.decode(dataValue.getValue().array()) : null;

      final int blockId = partitionFunction.getBlockId(decodedKey);
      final DataOpMetadata<K, V> operation = new DataOpMetadata<>(origEvalId,
          operationId, opType, tableId, blockId, decodedKey, decodedValue);

      LOG.log(Level.FINEST, "Enqueue Op. OpId: {0}", operation.getOpId());
      try {
        operationQueue.put(operation);
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
      }
    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  private <K, V> void onTableAccessResMsg(final TableAccessMsg msg) {
    final TableAccessResMsg tableAccessResMsg = msg.getTableAccessResMsg();
    final long operationId = msg.getOperationId();
    final DataValue remoteOutput = tableAccessResMsg.getDataValue();
    final boolean isSuccess = tableAccessResMsg.getIsSuccess();

    @SuppressWarnings("unchecked")
    final RemoteDataOp<K, V> operation = ongoingOp.get(operationId);
    if (operation == null) {
      LOG.log(Level.WARNING, "The operation is already handled or cancelled due to timeout. OpId: {0}", operationId);
      return;
    }

    try {
      final TableComponents<K, V> tableComponents = tables.get(operation.getMetadata().getTableId());
      final KVSerializer<K, V> kvSerializer = tableComponents.getSerializer();
      final Codec<V> valueCodec = kvSerializer.getValueCodec();

      // decode data value
      final V decodedValue = isSuccess && remoteOutput != null ?
          valueCodec.decode(remoteOutput.getValue().array()) : null;

      operation.commitResult(decodedValue, isSuccess);

      LOG.log(Level.FINEST, "Remote operation is finished. OpId: {0}", operationId);
    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Registers an operation before sending it to remote memory store.
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

  /**
   * Sends the result to the original store.
   */
  private <K, V> void sendResultToOrigin(final DataOpMetadata<K, V> operation,
                                         @Nullable final V localOutput,
                                         final boolean isSuccess) {
    LOG.log(Level.FINEST, "Send result to origin. OpId: {0}, OrigId: {1}",
        new Object[]{operation.getOpId(), operation.getOrigId()});

    final String tableId = operation.getTableId();

    final TableComponents<K, V> tableComponents;
    try {
      tableComponents = tables.get(tableId);

      final Codec<V> valueCodec = tableComponents.getSerializer().getValueCodec();
      final String origEvalId = operation.getOrigId();

      final DataValue dataValue;
      if (localOutput != null) {
        dataValue = new DataValue(ByteBuffer.wrap(valueCodec.encode(localOutput)));
      } else {
        dataValue = null;
      }

      msgSenderFuture.get().sendTableAccessResMsg(origEvalId, operation.getOpId(), dataValue, isSuccess);

    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }
}
