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
import edu.snu.cay.services.et.configuration.parameters.NumRemoteOpsHandlerThreads;
import edu.snu.cay.services.et.evaluator.api.BlockPartitioner;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.exceptions.BlockNotExistsException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that handles 1) remote access request msgs that other executors sent to this executor
 * and 2) sends the result of the operation to the origin.
 */
final class RemoteAccessOpHandler {
  private static final Logger LOG = Logger.getLogger(RemoteAccessOpHandler.class.getName());
  private static final int QUEUE_SIZE = 1024;
  private static final int QUEUE_TIMEOUT_MS = 3000;

  /**
   * A queue for operations requested from remote clients.
   */
  private final BlockingQueue<DataOpMetadata> operationQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);

  private volatile boolean closeFlag = false;

  private final InjectionFuture<Tables> tablesFuture;
  private final InjectionFuture<MessageSender> msgSenderFuture;

  @Inject
  private RemoteAccessOpHandler(final InjectionFuture<Tables> tablesFuture,
                                @Parameter(NumRemoteOpsHandlerThreads.class) final int numRemoteThreads,
                                final InjectionFuture<MessageSender> msgSenderFuture) {
    this.tablesFuture = tablesFuture;
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

  void close() {
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

    private <K, V, U> void handleOperation(final DataOpMetadata<K, V, U> operation) {
      final String tableId = operation.getTableId();
      final int blockId = operation.getBlockId();

      LOG.log(Level.FINEST, "Poll op: [OpId: {0}, origId: {1}, block: {2}]]",
          new Object[]{operation.getOpId(), operation.getOrigId(), blockId});

      try {
        final TableComponents<K, V, U> tableComponents = tablesFuture.get().getTableComponents(tableId);
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
            case PUT_IF_ABSENT:
              output = block.putIfAbsent(operation.getKey(), operation.getValue().get());
              break;
            case GET:
              output = block.get(operation.getKey());
              break;
            case REMOVE:
              output = block.remove(operation.getKey());
              break;
            case UPDATE:
              output = block.update(operation.getKey(), operation.getUpdateValue().get(),
                  tableComponents.getUpdateFunction());
              break;
            default:
              LOG.log(Level.WARNING, "Undefined type of operation.");
              output = null;
              isSuccess = false;
            }

            sendResultToOrigin(operation, output, isSuccess);
          } else {
            LOG.log(Level.WARNING,
                "Failed to execute operation {0} requested by remote executor {2}." +
                    " This executor was considered as the owner of block {1} by executor {2}," +
                    " but the local ownership cache assumes executor {3} is the owner",
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
   * Handles the data operation sent from the remote executor.
   */
  <K, V, U> void onTableAccessReqMsg(final long opId, final TableAccessReqMsg msg) {
    final String origEvalId = msg.getOrigId();
    final OpType opType = msg.getOpType();
    final String tableId = msg.getTableId();
    final DataKey dataKey = msg.getDataKey();
    final DataValue dataValue = msg.getDataValue();

    try {
      final TableComponents<K, V, U> tableComponents = tablesFuture.get().getTableComponents(tableId);
      final KVUSerializer<K, V, U> kvuSerializer = tableComponents.getSerializer();
      final Codec<K> keyCodec = kvuSerializer.getKeyCodec();
      final Codec<V> valueCodec = kvuSerializer.getValueCodec();
      final Codec<U> updateValueCodec = kvuSerializer.getUpdateValueCodec();
      final BlockPartitioner<K> blockPartitioner = tableComponents.getBlockPartitioner();

      // decode data keys
      final K decodedKey = keyCodec.decode(dataKey.getKey().array());

      // decode data values
      final V decodedValue = opType.equals(OpType.PUT) || opType.equals(OpType.PUT_IF_ABSENT) ?
          valueCodec.decode(dataValue.getValue().array()) : null;

      // decode update data value
      final U decodedUpdateValue = opType.equals(OpType.UPDATE) ?
          updateValueCodec.decode(dataValue.getValue().array()) : null;

      final int blockId = blockPartitioner.getBlockId(decodedKey);
      final DataOpMetadata<K, V, U> operation = new DataOpMetadata<>(origEvalId,
          opId, opType, tableId, blockId, decodedKey, decodedValue, decodedUpdateValue);

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

  /**
   * Sends the result to the original executor.
   */
  private <K, V> void sendResultToOrigin(final DataOpMetadata<K, V, ?> operation,
                                         @Nullable final V localOutput,
                                         final boolean isSuccess) {
    LOG.log(Level.FINEST, "Send result to origin. OpId: {0}, OrigId: {1}",
        new Object[]{operation.getOpId(), operation.getOrigId()});

    final String tableId = operation.getTableId();

    final TableComponents<K, V, ?> tableComponents;
    try {
      tableComponents = tablesFuture.get().getTableComponents(tableId);

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
