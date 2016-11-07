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

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.KeyCodecName;
import edu.snu.cay.services.em.evaluator.api.MigrationExecutor;
import edu.snu.cay.services.em.evaluator.api.RemoteAccessibleMemoryStore;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.htrace.Span;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Ownership-first version of {@link MigrationExecutor}.
 * It preserves updates on data values during migration by blocking accesses on data.
 *
 * Protocol:
 * 1) MoveInitMsg (Driver -> sender)
 * 2) OwnershipMsg & DataMsg (sender -> receiver): as they are sent asynchronously,
 * step 3-4 can have two parallel paths (A/B)
 * 3A) OwnershipAckMsg (receiver -> sender)
 * 4A) OwnershipAckMsg (sender -> Driver)
 * 3B) DataAckMsg (receiver -> sender)
 * 4B) BlockMovedMsg (sender -> Driver)
 * The executor takes in charge of tracking the states regardless of the message order.
 *
 * @param <K> Type of key in MemoryStore
 */
public final class OwnershipFirstMigrationExecutor<K> implements MigrationExecutor {
  private static final Logger LOG = Logger.getLogger(OwnershipFirstMigrationExecutor.class.getName());

  // limit the number of concurrent migrations to prevent too much gap between ownership transition and data transfer
  private static final int MAX_CONCURRENT_MIGRATIONS = 4;
  private static final int NUM_BLOCK_SENDER_THREADS = 2;
  private static final int NUM_DATA_MSG_HANDLER_THREADS = 2;
  private static final int NUM_OWNERSHIP_MSG_HANDLER_THREADS = 2;

  private final RemoteAccessibleMemoryStore<K> memoryStore;
  private final OperationRouter router;
  private final InjectionFuture<ElasticMemoryMsgSender> sender;

  // Thread pools to handle the messages in separate threads to prevent NCS threads' overhead.
  private final ExecutorService blockSenderExecutor = Executors.newFixedThreadPool(NUM_BLOCK_SENDER_THREADS);
  private final ExecutorService dataMsgHandlerExecutor = Executors.newFixedThreadPool(NUM_DATA_MSG_HANDLER_THREADS);
  private final ExecutorService ownershipMsgHandlerExecutor =
      Executors.newFixedThreadPool(NUM_OWNERSHIP_MSG_HANDLER_THREADS);

  private final Map<String, Migration> ongoingMigrations = new ConcurrentHashMap<>();

  /**
   * A map for maintaining state of incoming blocks in receiver.
   * It's for ownership-first migration in which the order of {@link OwnershipMsg} and {@link DataMsg} can be reversed.
   * Using this map, handlers of both msgs can judge whether it arrives first or not.
   * A later message will call {@link #processDataMsg} to put a received block into MemoryStore and
   * release client threads that were blocked by {@link #onOwnershipMsg(MigrationMsg)}.
   * A value is {@link Optional#empty()} when an entry is put by OwnershipMsg.
   */
  private final Map<Integer, Optional<Pair<Map<K, Object>, TraceInfo>>> msgArrivedBlocks = new ConcurrentHashMap<>();

  private final Codec<K> keyCodec;
  private final Serializer serializer;

  @Inject
  private OwnershipFirstMigrationExecutor(final RemoteAccessibleMemoryStore<K> memoryStore,
                                          final OperationRouter router,
                                          final InjectionFuture<ElasticMemoryMsgSender> sender,
                                          @Parameter(KeyCodecName.class)final Codec<K> keyCodec,
                                          final Serializer serializer) {
    this.memoryStore = memoryStore;
    this.router = router;
    this.sender = sender;
    this.keyCodec = keyCodec;
    this.serializer = serializer;
  }

  @Override
  public void onNext(final MigrationMsg msg) {
    switch (msg.getType()) {
    case MoveInitMsg:
      onMoveInitMsg(msg);
      break;

    case OwnershipMsg:
      onOwnershipMsg(msg);
      break;

    case OwnershipAckMsg:
      onOwnershipAckMsg(msg);
      break;

    case DataMsg:
      onDataMsg(msg);
      break;

    case DataAckMsg:
      onDataAckMsg(msg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onMoveInitMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final MoveInitMsg moveInitMsg = msg.getMoveInitMsg();
    final String senderId = moveInitMsg.getSenderId().toString();
    final String receiverId = moveInitMsg.getReceiverId().toString();
    final List<Integer> blockIds = moveInitMsg.getBlockIds();

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    Trace.setProcessId("src_eval");
    try (TraceScope onMoveInitMsgScope = Trace.startSpan("on_move_init_msg",
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      detached = onMoveInitMsgScope.detach();
      final TraceInfo traceInfo = TraceInfo.fromSpan(detached);

      final Migration migration = new Migration(operationId, senderId, receiverId, blockIds, traceInfo);
      ongoingMigrations.put(operationId, migration);

      for (int i = 0; i < NUM_BLOCK_SENDER_THREADS; i++) {
        blockSenderExecutor.submit(migration::startMigratingBlock);
      }
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  private void onOwnershipMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final OwnershipMsg ownershipMsg = msg.getOwnershipMsg();
    final int blockId = ownershipMsg.getBlockId();
    final String senderId = ownershipMsg.getSenderId().toString();
    final int oldOwnerId = ownershipMsg.getOldOwnerId();
    final int newOwnerId = ownershipMsg.getNewOwnerId();

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    Trace.setProcessId("dst_eval");
    try (TraceScope onOwnershipMsgScope = Trace.startSpan(String.format("on_ownership_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      detached = onOwnershipMsgScope.detach();
      final TraceInfo traceInfo = TraceInfo.fromSpan(detached);

      // should run asynchronously to prevent deadlock in router.updateOwnership()
      ownershipMsgHandlerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          // clients should wait until DataMsg arrives
          router.markBlockAsMigrating(blockId);

          // In ownership-first migration, the order of OwnershipMsg and DataMsg is not fixed.
          // However, DataMsg should be handled after updating ownership by OwnershipAckMsg.
          // So if DataMsg for the same block has been already arrived, handle that msg now.
          final boolean ownershipMsgArrivedFirst;
          Pair<Map<K, Object>, TraceInfo> dataMsgInfo = null;
          synchronized (msgArrivedBlocks) {
            ownershipMsgArrivedFirst = !msgArrivedBlocks.containsKey(blockId);
            if (ownershipMsgArrivedFirst) {
              msgArrivedBlocks.put(blockId, Optional.empty());
            } else {
              dataMsgInfo = msgArrivedBlocks.remove(blockId).get();
            }
          }

          // Update the owner of the block to the new one.
          // It waits until all operations release a read-lock on router and acquires write-lock
          router.updateOwnership(blockId, oldOwnerId, newOwnerId);

          sender.get().sendOwnershipAckMsg(Optional.of(senderId), operationId, blockId, oldOwnerId, newOwnerId,
                  traceInfo);

          // If the messages have arrived out of order, there should be an awaiting DataMsg to be processed.
          if (!ownershipMsgArrivedFirst) {
            processDataMsg(operationId, senderId, blockId, dataMsgInfo.getFirst(), dataMsgInfo.getSecond());
          }
        }
      });
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  private void onOwnershipAckMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final OwnershipAckMsg ownershipAckMsg = msg.getOwnershipAckMsg();
    final int blockId = ownershipAckMsg.getBlockId();
    final int oldOwnerId = ownershipAckMsg.getOldOwnerId();
    final int newOwnerId = ownershipAckMsg.getNewOwnerId();

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    Trace.setProcessId("src_eval");
    try (TraceScope onOwnershipAckMsgScope = Trace.startSpan(
        String.format("on_ownership_ack_msg. blockId: %d", blockId), HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      detached = onOwnershipAckMsgScope.detach();
      final TraceInfo traceInfo = TraceInfo.fromSpan(detached);

      // should run asynchronously to prevent deadlock in router.updateOwnership()
      ownershipMsgHandlerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          // Update the owner of the block to the new one.
          // Operations being executed keep a read lock on router while being executed.
          router.updateOwnership(blockId, oldOwnerId, newOwnerId);

          // Release block that was marked in Migration.startMigratingBlock()
          // It wakes up awaiting client threads to access emigrated data via remote access
          router.releaseMigratedBlock(blockId);

          sender.get().sendOwnershipAckMsg(Optional.empty(), operationId, blockId, oldOwnerId, newOwnerId, traceInfo);
        }
      });
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  private void onDataMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final DataMsg dataMsg = msg.getDataMsg();
    final String senderId = dataMsg.getSenderId().toString();
    final int blockId = dataMsg.getBlockId();

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    Trace.setProcessId("dst_eval");
    try (TraceScope onDataMsgScope = Trace.startSpan(String.format("on_data_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      detached = onDataMsgScope.detach();
      final TraceInfo traceInfo = TraceInfo.fromSpan(detached);

      dataMsgHandlerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          final Map<K, Object> dataMap;
          try (TraceScope decodeDataScope = Trace.startSpan("decode_data", traceInfo)) {
            dataMap = toDataMap(dataMsg.getKeyValuePairs(), serializer.getCodec());
          }

          // In ownership-first migration, the order of OwnershipMsg and DataMsg is not fixed.
          // However, DataMsg should be handled after updating ownership by OwnershipMsg.
          // So handle DataMsg now, if OwnershipMsg for the same block has been already arrived.
          // Otherwise handle it in future when corresponding OwnershipMsg arrives
          final boolean ownershipMsgArrivedFirst;
          synchronized (msgArrivedBlocks) {
            ownershipMsgArrivedFirst = msgArrivedBlocks.containsKey(blockId);
            if (ownershipMsgArrivedFirst) {
              msgArrivedBlocks.remove(blockId);
            } else {
              msgArrivedBlocks.put(blockId, Optional.of(new Pair<>(dataMap, traceInfo)));
            }
          }

          // If the messages have arrived in order, DataMsg should be processed here.
          if (ownershipMsgArrivedFirst) {
            processDataMsg(operationId, senderId, blockId, dataMap, traceInfo);
          }
        }
      });
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  private void processDataMsg(final String operationId, final String senderId,
                              final int blockId, final Map<K, Object> dataMap, final TraceInfo traceInfo) {
    memoryStore.putBlock(blockId, dataMap);

    // Unmark block marked in onOwnershipMsg.
    // It wakes up waiting client threads to access immigrated data.
    router.releaseMigratedBlock(blockId);

    sender.get().sendDataAckMsg(senderId, blockId, operationId, traceInfo);
  }

  private void onDataAckMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final DataAckMsg dataAckMsg = msg.getDataAckMsg();
    final int blockId = dataAckMsg.getBlockId();

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    Trace.setProcessId("src_eval");
    try (TraceScope onDataAckMsgScope = Trace.startSpan(String.format("on_data_ack_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      detached = onDataAckMsgScope.detach();
      final TraceInfo traceInfo = TraceInfo.fromSpan(detached);

      final Migration migration = ongoingMigrations.get(operationId);
      if (migration.finishMigratingBlock()) {
        ongoingMigrations.remove(operationId, migration);
      }

      dataMsgHandlerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          // After the data is migrated, it's safe to remove the local data block.
          memoryStore.removeBlock(blockId);

          sender.get().sendBlockMovedMsg(operationId, blockId, traceInfo);
        }
      });
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  /**
   * Abstraction of a single migration for managing states
   * while sending Ownership and Data msgs and receiving corresponding ack msgs.
   */
  private final class Migration {
    // metadata of migration
    private final String operationId;
    private final String senderId;
    private final String receiverId;
    private final List<Integer> blockIds;
    private final TraceInfo parentTraceInfo;

    // state of migration
    private final AtomicInteger blockIdxCounter = new AtomicInteger(0);
    private final AtomicInteger migratedBlockCounter = new AtomicInteger(0);

    // semaphore to restrict the number of concurrent block migration
    private final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_MIGRATIONS);

    Migration(final String operationId, final String senderId, final String receiverId, final List<Integer> blockIds,
              final TraceInfo parentTraceInfo) {
      this.operationId = operationId;
      this.senderId = senderId;
      this.receiverId = receiverId;
      this.blockIds = Collections.unmodifiableList(blockIds);
      this.parentTraceInfo = parentTraceInfo;
    }

    private void startMigratingBlock() {
      int blockIdxToSend = blockIdxCounter.getAndIncrement();

      while (blockIdxToSend < blockIds.size()) {
        // can progress after acquiring a permit
        semaphore.acquireUninterruptibly();

        final int blockIdToMigrate = blockIds.get(blockIdxToSend);

        LOG.log(Level.FINE, "Start migrating a block. numTotalBlocksToSend: {0}, numSentBlocks: {1}," +
            " senderId: {2}, receiverId: {3}, blockId: {4}",
            new Object[]{blockIds.size(), blockIdxToSend, senderId, receiverId, blockIdToMigrate});

        try (TraceScope sendingBlockScope = Trace.startSpan(
            String.format("send_block. blockId: %d", blockIdToMigrate), parentTraceInfo)) {
          final TraceInfo traceInfo = TraceInfo.fromSpan(sendingBlockScope.getSpan());

          // stop clients's access before starting migration
          router.markBlockAsMigrating(blockIdToMigrate);

          final Map<K, Object> blockData = memoryStore.getBlock(blockIdToMigrate);
          final List<KeyValuePair> keyValuePairs;
          try (TraceScope decodeDataScope = Trace.startSpan("encode_data", traceInfo)) {
            keyValuePairs = toKeyValuePairs(blockData, serializer.getCodec());
          }

          final int oldOwnerId = getStoreId(senderId);
          final int newOwnerId = getStoreId(receiverId);

          // send ownership msg and data msg at once
          sender.get().sendOwnershipMsg(Optional.of(receiverId), senderId, operationId,
              blockIdToMigrate, oldOwnerId, newOwnerId, traceInfo);
          sender.get().sendDataMsg(receiverId, keyValuePairs, blockIdToMigrate, operationId, traceInfo);

          blockIdxToSend = blockIdxCounter.getAndIncrement();
        }
      }
    }

    /**
     * Finish migration of a single block and let one thread to start migrating a next block.
     * @return True, if all blocks are migrated
     */
    private boolean finishMigratingBlock() {
      semaphore.release();
      return migratedBlockCounter.incrementAndGet() == blockIds.size();
    }
  }

  private <V> List<KeyValuePair> toKeyValuePairs(final Map<K, V> blockData,
                                                 final Codec<V> valueCodec) {
    final List<KeyValuePair> kvPairs = new ArrayList<>(blockData.size());
    for (final Map.Entry<K, V> entry : blockData.entrySet()) {
      kvPairs.add(KeyValuePair.newBuilder()
          .setKey(ByteBuffer.wrap(keyCodec.encode(entry.getKey())))
          .setValue(ByteBuffer.wrap(valueCodec.encode(entry.getValue())))
          .build());
    }
    return kvPairs;
  }

  private <V> Map<K, V> toDataMap(final List<KeyValuePair> keyValuePairs,
                                  final Codec<V> valueCodec) {
    final Map<K, V> dataMap = new HashMap<>(keyValuePairs.size());
    for (final KeyValuePair kvPair : keyValuePairs) {
      dataMap.put(
          keyCodec.decode(kvPair.getKey().array()),
          valueCodec.decode(kvPair.getValue().array()));
    }
    return dataMap;
  }
  
  /**
   * Converts evaluator id to store id.
   * TODO #509: remove assumption on the format of context Id
   */
  private int getStoreId(final String evalId) {
    // MemoryStoreId is the suffix of context id (Please refer to PartitionManager.registerEvaluator()
    // and ElasticMemoryConfiguration.getServiceConfigurationWithoutNameResolver()).
    return Integer.valueOf(evalId.split("-")[1]);
  }
}
