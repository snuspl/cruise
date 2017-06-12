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
import edu.snu.cay.services.et.configuration.parameters.remoteaccess.NumRemoteOpsSenderThreads;
import edu.snu.cay.services.et.configuration.parameters.remoteaccess.SenderQueueSize;
import edu.snu.cay.services.et.evaluator.api.DataOpResult;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that 1) sends remote access request msgs to a remote executor that owns data block
 * and 2) handles the result received from the executor.
 */

final class RemoteAccessOpSender {
  private static final Logger LOG = Logger.getLogger(RemoteAccessOpSender.class.getName());
  private static final int QUEUE_TIMEOUT_MS = 3000;
  private static final long RESEND_INTERVAL_MS = 100;

  /**
   * A counter for issuing ids for operations sent to remote executors.
   */
  private final AtomicLong remoteOpIdCounter = new AtomicLong(0);

  private final List<SenderThread> senderThreads;

  private final int numSenderThreads;

  /**
   * A map holding ongoing operations until they finish.
   * It only maintains operations requested from local clients.
   */
  private final Map<String, Map<Long, RemoteDataOp>> tableIdToOngoingOps = new ConcurrentHashMap<>();

  /**
   * A boolean flag that becomes true when {@link #close()} is called,
   * which consequently terminates all sender threads.
   */
  private volatile boolean closeFlag = false;

  private final InjectionFuture<Tables> tablesFuture;
  private final String executorId;
  private final InjectionFuture<MessageSender> msgSenderFuture;
  private final InjectionFuture<RemoteAccessOpStat> networkUsageStatFuture;

  @Inject
  private RemoteAccessOpSender(final InjectionFuture<Tables> tablesFuture,
                               @Parameter(ExecutorIdentifier.class) final String executorId,
                               @Parameter(SenderQueueSize.class) final int queueSize,
                               @Parameter(NumRemoteOpsSenderThreads.class) final int numSenderThreads,
                               final InjectionFuture<MessageSender> msgSenderFuture,
                               final InjectionFuture<RemoteAccessOpStat> networkUsageStatFuture) {
    this.tablesFuture = tablesFuture;
    this.executorId = executorId;
    this.numSenderThreads = numSenderThreads;
    this.msgSenderFuture = msgSenderFuture;
    this.networkUsageStatFuture = networkUsageStatFuture;
    this.senderThreads = initSenderThreads(numSenderThreads, queueSize);
  }

  /**
   * Initialize {@link SenderThread}s that execute operations sequentially in their own local queue.
   * They send remote access request messages to an executor that owns a block that contains a key of an operation.
   */
  private List<SenderThread> initSenderThreads(final int numThreads, final int queueSize) {
    LOG.log(Level.INFO, "Initializing {0} Sender threads with queue size: {1}",
        new Object[]{numThreads, queueSize});
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    final List<SenderThread> threads = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final SenderThread senderThread = new SenderThread(queueSize);
      threads.add(senderThread);
      executorService.submit(senderThread);
    }
    return threads;
  }

  /**
   * Close this {@link RemoteAccessOpSender} by terminating all sender threads.
   */
  void close() {
    closeFlag = true;
  }

  /**
   * A thread abstraction that sending messages in parallel.
   * Several threads are initiated at the beginning and run as long-running background services.
   * Each thread takes charge of a disjoint set of key-space (See {@link #getThreadIdx(int)}).
   */
  private final class SenderThread implements Runnable {
    private static final int DRAIN_PORTION = 16;
    private final BlockingQueue<RemoteDataOp> opQueue;

    // Operations drained from the opQueue, and processed locally.
    private final ArrayList<RemoteDataOp> localOps;

    private final int drainSize;

    /**
     * @param queueSize a size of a thread queue
     */
    SenderThread(final int queueSize) {
      this.opQueue = new ArrayBlockingQueue<>(queueSize);
      this.drainSize = queueSize >= DRAIN_PORTION ? queueSize / DRAIN_PORTION : 1;
      this.localOps = new ArrayList<>(drainSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      try {
        while (!closeFlag) {
          try {
            final RemoteDataOp op = opQueue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (op == null) {
              continue;
            }

            processOp(op);
          } catch (InterruptedException e) {
            continue;
          }

          opQueue.drainTo(localOps, drainSize);
          localOps.forEach(this::processOp);
          localOps.clear();
        }

        // catch and rethrow RuntimeException after leaving a log
        // otherwise, the thread disappears without any noticeable marks
      } catch (final Exception e) {
        LOG.log(Level.SEVERE, "Sender thread has been down due to unexpected exception", e);
        throw new RuntimeException(e);
      }
    }

    private <K, V, U> void processOp(final RemoteDataOp<K, V, U> op) {
      final DataOpMetadata opMetadata = op.getMetadata();
      final String tableId = opMetadata.getTableId();

      LOG.log(Level.FINEST, "Process op: [OpId: {0}, origId: {1}, table: {2}]]",
          new Object[]{opMetadata.getOpId(), opMetadata.getOrigId(), tableId});

      final TableComponents<K, V, U> tableComponents;

      try {
        tableComponents = tablesFuture.get().getTableComponents(tableId);
      } catch (TableNotExistException e) {
        throw new RuntimeException(e);
      }
      encodeAndSendRequestMsg(opMetadata, op.getTargetId(), executorId, tableComponents, msgSenderFuture.get());

      // for operations that require replies, deregister them when receiving the reply
      if (!opMetadata.isReplyRequired()) {
        deregisterOp(tableId, opMetadata.getOpId());
      }
    }

    /**
     * Enqueue operation into a thread's queue.
     * The operations will be processed sequentially by this thread.
     */
    void enqueue(final RemoteDataOp op) {
      LOG.log(Level.FINEST, "Enqueue Op. OpId: {0}, origId: {1}",
          new Object[]{op.getMetadata().getOpId(), op.getMetadata().getOrigId()});

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
   * Encode values and send a request message to a target executor.
   * @param opMetadata {@link SingleKeyDataOpMetadata}
   * @param targetExecutorId a target executor Id
   * @param tableComponents {@link TableComponents}
   * @param msgSender {@link MessageSender}
   */
  static <K, V, U> void encodeAndSendRequestMsg(final DataOpMetadata opMetadata,
                                                final String targetExecutorId,
                                                final String localExecutorId,
                                                final TableComponents<K, V, U> tableComponents,
                                                final MessageSender msgSender) {

    final KVUSerializer<K, V, U> kvuSerializer = tableComponents.getSerializer();
    if (opMetadata.isSingleKey()) {
      final SingleKeyDataOpMetadata<K, V, U> singleOpMetadata = (SingleKeyDataOpMetadata) opMetadata;
      final Pair<DataKey, DataValue> singleKVPair = encodeSingleKeyOp(kvuSerializer, singleOpMetadata);
      sendSingleKeyReqMsg(opMetadata, targetExecutorId, localExecutorId, tableComponents, msgSender, singleKVPair);
    } else {
      final MultiKeyDataOpMetadata<K, V, U> multiKeyDataOpMetadata = (MultiKeyDataOpMetadata) opMetadata;
      final Pair<DataKeys, DataValues> multiKVPair = encodeMultiKeyOp(kvuSerializer, multiKeyDataOpMetadata);
      sendMultiKeyReqMsg(opMetadata, targetExecutorId, localExecutorId, tableComponents, msgSender, multiKVPair);
    }
  }

  private static void sendSingleKeyReqMsg(final DataOpMetadata opMetadata,
                                          final String targetExecutorId,
                                          final String localExecutorId,
                                          final TableComponents tableComponents,
                                          final MessageSender msgSender,
                                          final Pair<DataKey, DataValue> kvPair) {
    String executorIdToSendMsg = targetExecutorId;
    final OwnershipCache ownershipCache = tableComponents.getOwnershipCache();
    while (true) {
      try {
        msgSender.sendTableAccessReqMsg(opMetadata.getOrigId(), executorIdToSendMsg, opMetadata.getOpId(),
            opMetadata.getTableId(), opMetadata.getOpType(), opMetadata.isReplyRequired(),
            kvPair.getKey(), kvPair.getValue());
        break;
      } catch (NetworkException e) {
        LOG.log(Level.WARNING, "NetworkException while sending a msg. Resend", e);
      }

      LOG.log(Level.INFO, "Wait {0} ms before resending a msg", RESEND_INTERVAL_MS);
      try {
        // may not sleep for RESEND_INTERVAL_MS due to interrupt
        Thread.sleep(RESEND_INTERVAL_MS);
      } catch (final InterruptedException e) {
        LOG.log(Level.FINEST, "Interrupted while waiting for ownership cache to be updated", e);
      }

      // re-resolve target executor id on fail
      final Optional<String> targetIdOptional = ownershipCache.resolveExecutor(opMetadata.getBlockId());

      // send to local when it's migrated into local
      executorIdToSendMsg = targetIdOptional.orElse(localExecutorId);
    }
  }

  private static void sendMultiKeyReqMsg(final DataOpMetadata opMetadata,
                                         final String targetExecutorId,
                                         final String localExecutorId,
                                         final TableComponents tableComponents,
                                         final MessageSender msgSender,
                                         final Pair<DataKeys, DataValues> kvPair) {
    String executorIdToSendMsg = targetExecutorId;
    final OwnershipCache ownershipCache = tableComponents.getOwnershipCache();
    while (true) {
      try {
        msgSender.sendTableAccessReqMsg(opMetadata.getOrigId(), executorIdToSendMsg, opMetadata.getOpId(),
            opMetadata.getTableId(), opMetadata.getOpType(), opMetadata.isReplyRequired(),
            kvPair.getKey(), kvPair.getValue());
        break;
      } catch (NetworkException e) {
        LOG.log(Level.WARNING, "NetworkException while sending a msg. Resend", e);
      }

      LOG.log(Level.INFO, "Wait {0} ms before resending a msg", RESEND_INTERVAL_MS);
      try {
        // may not sleep for RESEND_INTERVAL_MS due to interrupt
        Thread.sleep(RESEND_INTERVAL_MS);
      } catch (final InterruptedException e) {
        LOG.log(Level.FINEST, "Interrupted while waiting for ownership cache to be updated", e);
      }

      // re-resolve target executor id on fail
      final Optional<String> targetIdOptional = ownershipCache.resolveExecutor(opMetadata.getBlockId());

      // send to local when it's migrated into local
      executorIdToSendMsg = targetIdOptional.orElse(localExecutorId);
    }
  }

  private static <K, V, U> Pair<DataKey, DataValue> encodeSingleKeyOp(
      final KVUSerializer<K, V, U> kvuSerializer,
      final SingleKeyDataOpMetadata<K, V, U> dataOpMetadata) {

    final DataKey dataKey = new DataKey();
    final DataValue dataValue;
    final Codec<K> keyCodec = kvuSerializer.getKeyCodec();
    final Codec<V> valueCodec = kvuSerializer.getValueCodec();
    final Codec<U> updateValueCodec = kvuSerializer.getUpdateValueCodec();

    // encode key
    final ByteBuffer encodedKey = ByteBuffer.wrap(keyCodec.encode(dataOpMetadata.getKey()));
    dataKey.setKey(encodedKey);

    // encode value
    if (dataOpMetadata.getOpType().equals(OpType.PUT) || dataOpMetadata.getOpType().equals(OpType.PUT_IF_ABSENT)) {
      if (!dataOpMetadata.getValue().isPresent()) {
        throw new RuntimeException(String.format("Data value is empty for PUT(%s)",
            dataOpMetadata.getKey().toString()));
      }
      final ByteBuffer encodedValue = ByteBuffer.wrap(
          valueCodec.encode(dataOpMetadata.getValue().get()));
      dataValue = new DataValue();
      dataValue.setValue(encodedValue);
    } else if (dataOpMetadata.getOpType().equals(OpType.UPDATE)) {
      if (!dataOpMetadata.getUpdateValue().isPresent()) {
        throw new RuntimeException(String.format("Data value is empty for UPDATE(%s)",
            dataOpMetadata.getKey().toString()));
      }
      final ByteBuffer encodedUpdateValue = ByteBuffer.wrap(
          updateValueCodec.encode(dataOpMetadata.getUpdateValue().get()));
      dataValue = new DataValue();
      dataValue.setValue(encodedUpdateValue);
    } else {
      dataValue = null;
    }

    return Pair.of(dataKey, dataValue);
  }

  private static <K, V, U> Pair<DataKeys, DataValues> encodeMultiKeyOp(
      final KVUSerializer<K, V, U> kvuSerializer,
      final MultiKeyDataOpMetadata<K, V, U> dataOpMetadata) {
    final DataKeys dataKeys = new DataKeys();
    final DataValues dataValues = new DataValues();
    final Codec<K> keyCodec = kvuSerializer.getKeyCodec();
    final Codec<V> valueCodec = kvuSerializer.getValueCodec();
    final Codec<U> updateValueCodec = kvuSerializer.getUpdateValueCodec();

    final List<ByteBuffer> encodedKeys = new ArrayList<>();
    dataOpMetadata.getKeys().forEach(key -> encodedKeys.add(ByteBuffer.wrap(keyCodec.encode(key))));
    dataKeys.setKeys(encodedKeys);

    // use encodedValues for both data value and update value
    final List<ByteBuffer> encodedValues = new ArrayList<>();
    if (dataOpMetadata.getOpType().equals(OpType.PUT)) {
      if (dataOpMetadata.getValues().isEmpty()) {
        LOG.log(Level.SEVERE, "Data value is empty for PUT({0})", dataOpMetadata.getKeys());
        throw new RuntimeException(String.format("Data value is empty for PUT(%s)",
            dataOpMetadata.getKeys().toString()));
      }
      dataOpMetadata.getValues().forEach(value ->
          encodedValues.add(ByteBuffer.wrap(valueCodec.encode(value))));
      dataValues.setValues(encodedValues);

    } else if (dataOpMetadata.getOpType().equals(OpType.UPDATE)) {
      if (dataOpMetadata.getUpdateValues().isEmpty()) {
        LOG.log(Level.SEVERE, "Update value is empty for UPDATE({0})", dataOpMetadata.getKeys());
        throw new RuntimeException(String.format("Data update value is empty for UPDATE(%s)",
            dataOpMetadata.getKeys().toString()));
      }
      dataOpMetadata.getUpdateValues().forEach(updateValue ->
          encodedValues.add(ByteBuffer.wrap(updateValueCodec.encode(updateValue))));
      dataValues.setValues(encodedValues);

    } else {
      //TODO #176: support multi-key versions of other op types (e.g. get, remove):372
      LOG.log(Level.SEVERE, "This operation has wrong Op type");
      throw new RuntimeException("This operation has wrong Op type.");
    }
    return Pair.of(dataKeys, dataValues);
  }


  /**
   * Send operation to remote evaluators. Note that this method is for single-key.
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
   */
  <K, V, U> void sendSingleKeyOpToRemote(final OpType opType,
                                         final String tableId, final int blockId,
                                         final K key,
                                         @Nullable final V value,
                                         @Nullable final U updateValue,
                                         final String targetEvalId,
                                         final boolean replyRequired,
                                         final DataOpResult<V> dataOpResult) {
    final long operationId = remoteOpIdCounter.getAndIncrement();
    final RemoteDataOp<K, V, U> operation = new RemoteDataOp<>(executorId, targetEvalId,
        operationId, opType, replyRequired, tableId, blockId, key, value, updateValue, dataOpResult);
    final DataOpMetadata opMetadata = operation.getMetadata();

    LOG.log(Level.FINEST, "Send op to remote. OpId: {0}, OpType: {1}, targetId: {2}",
        new Object[]{opMetadata.getOpId(), opMetadata.getOpType(), targetEvalId});

    registerOp(operation);

    final int threadIdx = getThreadIdx(blockId);
    senderThreads.get(threadIdx).enqueue(operation);
  }

  /**
   * Send operation to remote evaluators. Note that this method is for multi-key.
   */
  <K, V, U> void sendMultiKeyOpToRemote(final OpType opType,
                                        final String tableId, final int blockId,
                                        final List<K> keyList, final List<V> valueList,
                                        final List<U> updateValueList,
                                        final String targetEvalId,
                                        final boolean replyRequired,
                                        final DataOpResult<Map<K, V>> aggregateDataOpResult) {

    final long operationId = remoteOpIdCounter.getAndIncrement();
    final RemoteDataOp<K, V, U> operation = new RemoteDataOp<>(executorId, targetEvalId, operationId,
        opType, replyRequired, tableId, blockId, keyList, valueList, updateValueList, aggregateDataOpResult);
    final DataOpMetadata opMetadata = operation.getMetadata();

    LOG.log(Level.FINER, "Send op to remote. OpId: {0}, OpType: {1}, targetId: {2}",
        new Object[]{opMetadata.getOpId(), opMetadata.getOpType(), targetEvalId});

    registerOp(operation);
    final int threadIdx = getThreadIdx(blockId);
    senderThreads.get(threadIdx).enqueue(operation);
  }

  private int getThreadIdx(final int hashedKey) {
    return hashedKey % numSenderThreads;
  }

  /**
   * Handles the result of remote operation.
   */
  @SuppressWarnings("unchecked")
  <K, V> void onTableAccessResMsg(final long opId, final TableAccessResMsg msg) {
    final String tableId = msg.getTableId();
    final boolean isSuccess = msg.getIsSuccess();

    final Map<Long, RemoteDataOp> ongoingOp = tableIdToOngoingOps.get(tableId);
    final RemoteDataOp operation = ongoingOp.get(opId);
    if (operation == null) {
      LOG.log(Level.WARNING, "The operation is already handled or cancelled due to timeout. OpId: {0}", opId);
      return;
    }

    try {
      final Codec<K> keyCodec = (Codec<K>) tablesFuture.get().getTableComponents(tableId)
          .getSerializer().getKeyCodec();
      final Codec<V> valueCodec = (Codec<V>) tablesFuture.get().getTableComponents(tableId)
          .getSerializer().getValueCodec();

      final OpType opType = operation.getMetadata().getOpType();
      if (operation.getMetadata().isSingleKey()) {
        final DataValue remoteDataValue = msg.getDataValue();
        final V decodedValue = isSuccess && remoteDataValue != null ?
            valueCodec.decode(remoteDataValue.getValue().array()) : null;

        // TODO #106: Collect metrics about all remote access operations
        if ((opType.equals(OpType.GET) || opType.equals(OpType.GET_OR_INIT)) && remoteDataValue != null) {
          networkUsageStatFuture.get().incBytesReceivedGetResp(tableId, remoteDataValue.getValue().array().length);
        }
        operation.getDataOpResult().onCompleted(decodedValue, isSuccess);
      } else {
        final List<ByteBuffer> encodedKeys = msg.getDataKeys().getKeys();
        final List<ByteBuffer> encodedValues = msg.getDataValues().getValues();
        final Map<K, V> decodedMap = new HashMap<>();
        for (int index = 0; index < encodedKeys.size(); index++) {
          final K key = keyCodec.decode(encodedKeys.get(index).array());
          final V value = valueCodec.decode(encodedValues.get(index).array());
          decodedMap.put(key, value);
        }
        operation.getDataOpResult().onCompleted(decodedMap, isSuccess);
      }

      deregisterOp(tableId, opId);
      LOG.log(Level.FINEST, "Remote operation is finished. OpId: {0}", opId);
    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Wait all ongoing operations for a given {@code tableId} to be finished.
   * Operations that require replies are finished after receive the response.
   * @param tableId a table id
   */
  void waitOpsTobeFlushed(final String tableId) {
    final Map ongoingOps = tableIdToOngoingOps.get(tableId);
    if (ongoingOps == null) {
      return;
    }

    LOG.log(Level.INFO, "Start waiting {0} ops to be flushed", ongoingOps.size());
    while (!ongoingOps.isEmpty()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore interrupt
      }
      LOG.log(Level.INFO, "ongoingOps.size(): {0}", ongoingOps.size());
    }
    tableIdToOngoingOps.remove(tableId);
    LOG.log(Level.INFO, "ops for {0} has been flushed out", tableId);
  }

  /**
   * Registers an operation before sending it to remote executor.
   */
  private void registerOp(final RemoteDataOp operation) {
    final Map<Long, RemoteDataOp> ongoingOp = tableIdToOngoingOps.computeIfAbsent(
        operation.getMetadata().getTableId(), key -> new ConcurrentHashMap<>());
    final RemoteDataOp unhandledOperation = ongoingOp.put(operation.getMetadata().getOpId(), operation);
    if (unhandledOperation != null) {
      LOG.log(Level.SEVERE, "Discard the exceptionally unhandled operation: {0}",
          unhandledOperation.getMetadata().getOpId());
    }
  }

  /**
   * Deregisters an operation after its remote access is finished.
   */
  private void deregisterOp(final String tableId, final long operationId) {
    final Map<Long, RemoteDataOp> ongoingOp = tableIdToOngoingOps.get(tableId);
    if (ongoingOp == null || ongoingOp.remove(operationId) == null) {
      LOG.log(Level.WARNING, "No ongoing ops with opId: {0}", operationId);
    }
  }
}

