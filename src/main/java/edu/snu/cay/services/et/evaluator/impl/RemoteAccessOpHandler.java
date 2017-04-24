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
import edu.snu.cay.services.et.configuration.parameters.HandlerQueueSize;
import edu.snu.cay.services.et.configuration.parameters.NumRemoteOpsHandlerThreads;
import edu.snu.cay.services.et.evaluator.api.BlockPartitioner;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.exceptions.BlockNotExistsException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that 1) handles remote access request msgs that other executors sent to this executor
 * and 2) sends the result of the operation to the origin.
 */
final class RemoteAccessOpHandler {
  private static final Logger LOG = Logger.getLogger(RemoteAccessOpHandler.class.getName());
  private static final int QUEUE_TIMEOUT_MS = 3000;

  private final int numHandlerThreads;

  private final List<HandlerThread> handlerThreads;

  /**
   * A map holding ongoing operations until they finish.
   * It maintains operations requested from remote clients.
   * Key is a table Id and value is a set of operation Id and original requester executor Id pair.
   * Operation should be identified with a pair of operation Id and and origin Id,
   * because operation Id is only unique within the original executor.
   */
  private final Map<String, Set<Pair<Long, String>>> tableIdToQueuedOps = new ConcurrentHashMap<>();

  private volatile boolean closeFlag = false;

  private final String executorId;
  private final InjectionFuture<Tables> tablesFuture;
  private final InjectionFuture<MessageSender> msgSenderFuture;

  @Inject
  private RemoteAccessOpHandler(final InjectionFuture<Tables> tablesFuture,
                                @Parameter(ExecutorIdentifier.class) final String executorId,
                                @Parameter(HandlerQueueSize.class) final int queueSize,
                                @Parameter(NumRemoteOpsHandlerThreads.class) final int numHandlerThreads,
                                final InjectionFuture<MessageSender> msgSenderFuture) {
    this.executorId = executorId;
    this.tablesFuture = tablesFuture;
    this.msgSenderFuture = msgSenderFuture;
    this.numHandlerThreads = numHandlerThreads;
    this.handlerThreads = initExecutor(numHandlerThreads, queueSize);
  }

  /**
   * Initialize threads that dequeue and execute operation from the {@code opQueue}.
   * That is, these threads serve operations requested from remote clients.
   */
  private List<HandlerThread> initExecutor(final int numThreads, final int queueSize) {
    LOG.log(Level.INFO, "Initializing {0} Handler threads", numThreads);
    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final List<HandlerThread> threads = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final HandlerThread handlerThread = new HandlerThread(queueSize);
      threads.add(handlerThread);
      executor.submit(handlerThread);
    }
    return threads;
  }

  /**
   * Close this {@link RemoteAccessOpHandler} by terminating all handler threads.
   */
  void close() {
    closeFlag = true;
  }

  /**
   * A runnable that handles operations requested from remote clients.
   * Several threads are initiated at the beginning and run as long-running background services.
   * Each thread takes charge of a disjoint set of key-space (See {@link #getThreadIdx(int)}).
   */
  private final class HandlerThread implements Runnable {
    private static final int DRAIN_PORTION = 16;
    private final BlockingQueue<DataOpMetadata> opQueue;

    // Operations drained from the opQueue, and processed locally.
    private final ArrayList<DataOpMetadata> localOps;

    private final int drainSize;

    /**
     * @param queueSize a size of a thread queue
     */
    HandlerThread(final int queueSize) {
      this.opQueue = new ArrayBlockingQueue<>(queueSize);
      this.drainSize = queueSize >= DRAIN_PORTION ? queueSize / DRAIN_PORTION : 1;
      this.localOps = new ArrayList<>(drainSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      try {
        while (!closeFlag) {
          // First, poll and execute a single operation.
          // Poll with a timeout will prevent busy waiting, when the queue is empty.
          try {
            final DataOpMetadata operation = opQueue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (operation == null) {
              continue;
            }

            processOp(operation);
          } catch (final InterruptedException e) {
            LOG.log(Level.SEVERE, "Poll failed with InterruptedException", e);
          }

          opQueue.drainTo(localOps, drainSize);
          localOps.forEach(this::processOp);
          localOps.clear();
        }

        // catch and rethrow RuntimeException after leaving a log
        // otherwise, the thread disappears without any noticeable marks
      } catch (final RuntimeException e) {
        LOG.log(Level.SEVERE, "Handler thread has been down due to RuntimeException", e);
        throw e;
      }
    }

    private <K, V, U> void processOp(final DataOpMetadata<K, V, U> operation) {
      final String tableId = operation.getTableId();
      final int blockId = operation.getBlockId();

      LOG.log(Level.FINEST, "Process op: [OpId: {0}, origId: {1}, tableId: {2}, blockId: {3}]]",
          new Object[]{operation.getOpId(), operation.getOrigId(), tableId, blockId});

      try {
        final TableComponents<K, V, U> tableComponents = tablesFuture.get().getTableComponents(tableId);
        final OwnershipCache ownershipCache = tableComponents.getOwnershipCache();
        final BlockStore<K, V, U> blockStore = tableComponents.getBlockStore();

        final Pair<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
        try {
          final V output;
          boolean isSuccess = true;

          final Optional<String> remoteEvalIdOptional = remoteEvalIdWithLock.getKey();
          final boolean isLocal = !remoteEvalIdOptional.isPresent();
          if (isLocal) {
            try {
              final BlockImpl<K, V, U> block = (BlockImpl<K, V, U>) blockStore.get(blockId);

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
              case GET_OR_INIT:
                output = block.getOrInit(operation.getKey());
                break;
              case REMOVE:
                output = block.remove(operation.getKey());
                break;
              case UPDATE:
                output = block.update(operation.getKey(), operation.getUpdateValue().get());
                break;
              default:
                LOG.log(Level.WARNING, "Undefined type of operation.");
                output = null;
                isSuccess = false;
              }
            } catch (final BlockNotExistsException e) {
              throw new RuntimeException(e);
            }

            if (operation.isReplyRequired()) {
              sendResultToOrigin(operation, output, isSuccess);
            }

          } else {
            // a case that operation comes to this executor based on wrong or stale ownership info
            redirect(operation, remoteEvalIdOptional.get());
          }
        } finally {
          final Lock ownershipLock = remoteEvalIdWithLock.getValue();
          ownershipLock.unlock();
        }

        deregisterOp(tableId, operation.getOpId(), operation.getOrigId());

      } catch (final TableNotExistException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Enqueue operation into a thread's queue.
     * The operations will be processed sequentially by this thread.
     */
    void enqueue(final DataOpMetadata op) {
      LOG.log(Level.FINEST, "Enqueue Op. OpId: {0}, origId: {1}", new Object[]{op.getOpId(), op.getOrigId()});

      while (true) {
        try {
          opQueue.put(op);
          break;
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "InterruptedException while enqueuing op", e);
        }
      }
    }
  }

 /**
   * Redirects an operation to the target executor.
   */
  private <K, V, U> void redirect(final DataOpMetadata<K, V, U> opMetadata, final String targetId) {
    try {
      final TableComponents<K, V, U> tableComponents = tablesFuture.get().getTableComponents(opMetadata.getTableId());

      LOG.log(Level.FINE, "Redirect Op for TableId: {0} / key: {1} to {2}",
          new Object[]{opMetadata.getTableId(), opMetadata.getKey(), targetId});
      RemoteAccessOpSender.encodeAndSendRequestMsg(opMetadata, targetId, executorId,
          tableComponents, msgSenderFuture.get());

    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Handles the data operation sent from the remote executor.
   */
  <K, V, U> void onTableAccessReqMsg(final long opId, final TableAccessReqMsg msg) {
    final String origEvalId = msg.getOrigId();
    final OpType opType = msg.getOpType();
    final boolean replyRequired = msg.getReplyRequired();
    final String tableId = msg.getTableId();
    final DataKey dataKey = msg.getDataKey();
    final DataValue dataValue = msg.getDataValue();

    registerOp(tableId, opId, origEvalId);

    try {
      final TableComponents<K, V, U> tableComponents = tablesFuture.get().getTableComponents(tableId);
      final KVUSerializer<K, V, U> kvuSerializer = tableComponents.getSerializer();
      final Codec<K> keyCodec = kvuSerializer.getKeyCodec();
      final Codec<V> valueCodec = kvuSerializer.getValueCodec();
      final Codec<U> updateValueCodec = kvuSerializer.getUpdateValueCodec();
      final BlockPartitioner<K> blockPartitioner = tableComponents.getBlockPartitioner();

      final EncodedKey<K> encodedKey = new EncodedKey<>(dataKey.getKey().array(), keyCodec);

      // decode data keys
      final K decodedKey = encodedKey.getKey();

      // decode data values
      final V decodedValue = opType.equals(OpType.PUT) || opType.equals(OpType.PUT_IF_ABSENT) ?
          valueCodec.decode(dataValue.getValue().array()) : null;

      // decode update data value
      final U decodedUpdateValue = opType.equals(OpType.UPDATE) ?
          updateValueCodec.decode(dataValue.getValue().array()) : null;

      final int blockId = blockPartitioner.getBlockId(encodedKey);
      final DataOpMetadata<K, V, U> operation = new DataOpMetadata<>(origEvalId,
          opId, opType, replyRequired, tableId, blockId, decodedKey, decodedValue, decodedUpdateValue);

      final int threadIdx = getThreadIdx(encodedKey.getHash());
      handlerThreads.get(threadIdx).enqueue(operation);

    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  private int getThreadIdx(final int hashedKey) {
    return hashedKey % numHandlerThreads;
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
    final String origEvalId = operation.getOrigId();

    final TableComponents<K, V, ?> tableComponents;
    try {
      tableComponents = tablesFuture.get().getTableComponents(tableId);

      final Codec<V> valueCodec = tableComponents.getSerializer().getValueCodec();

      final DataValue dataValue;
      if (localOutput != null) {
        dataValue = new DataValue(ByteBuffer.wrap(valueCodec.encode(localOutput)));
      } else {
        dataValue = null;
      }

      msgSenderFuture.get().sendTableAccessResMsg(origEvalId, operation.getOpId(), tableId, dataValue, isSuccess);

    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    } catch (NetworkException e) {
      LOG.log(Level.INFO, "The origin {0} has been removed, so the message is just discarded", origEvalId);
    }
  }

  /**
   * Wait all ongoing operations for a given {@code tableId} to be finished.
   * @param tableId a table id
   */
  void waitOpsTobeFlushed(final String tableId) {
    final Set remainingOps = tableIdToQueuedOps.get(tableId);
    if (remainingOps == null) {
      return;
    }

    LOG.log(Level.INFO, "Start waiting {0} ops to be flushed", remainingOps.size());
    while (!remainingOps.isEmpty()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore interrupt
      }
      LOG.log(Level.INFO, "remainingOps.size(): {0}", remainingOps.size());
    }
    tableIdToQueuedOps.remove(tableId);
    LOG.log(Level.INFO, "ops for {0} has been flushed out", tableId);
  }

  /**
   * Registers an operation before.
   */
  private void registerOp(final String tableId, final long opId, final String origId) {
    tableIdToQueuedOps.compute(tableId, (k, v) -> {
      final Set<Pair<Long, String>> queuedOps = v == null ?
          Collections.newSetFromMap(new ConcurrentHashMap<>()) : v;

      queuedOps.add(Pair.of(opId, origId));
      return queuedOps;
    });
  }

  /**
   * Deregisters an operation after processing it.
   */
  private void deregisterOp(final String tableId, final long opId, final String origId) {
    final Set<Pair<Long, String>> queuedOps = tableIdToQueuedOps.get(tableId);
    if (queuedOps == null || !queuedOps.remove(Pair.of(opId, origId))) {
      LOG.log(Level.WARNING, "No existing ops with opId: {0}, origId: {1}", new Object[]{opId, origId});
    }
  }
}
