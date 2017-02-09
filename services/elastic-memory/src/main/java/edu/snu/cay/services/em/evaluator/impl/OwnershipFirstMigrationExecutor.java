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
import edu.snu.cay.services.em.evaluator.api.BlockHandler;
import edu.snu.cay.services.em.evaluator.api.MigrationExecutor;
import edu.snu.cay.services.em.msg.api.EMMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.utils.trace.HTraceUtils;
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

  private final BlockHandler blockHandler;
  private final OwnershipCache ownershipCache;
  private final InjectionFuture<EMMsgSender> sender;

  // Thread pools to handle the messages in separate threads to prevent NCS threads' overhead.
  private final ExecutorService blockSenderExecutor = Executors.newFixedThreadPool(NUM_BLOCK_SENDER_THREADS);
  private final ExecutorService dataMsgHandlerExecutor = Executors.newFixedThreadPool(NUM_DATA_MSG_HANDLER_THREADS);
  private final ExecutorService ownershipMsgHandlerExecutor =
      Executors.newFixedThreadPool(NUM_OWNERSHIP_MSG_HANDLER_THREADS);

  // ongoing migrations in sender-side
  private final Map<String, Migration> ongoingMigrations = new ConcurrentHashMap<>();

  private final Codec<K> keyCodec;
  private final Serializer serializer;

  @Inject
  private OwnershipFirstMigrationExecutor(final BlockHandler blockHandler,
                                          final OwnershipCache ownershipCache,
                                          final InjectionFuture<EMMsgSender> sender,
                                          @Parameter(KeyCodecName.class)final Codec<K> keyCodec,
                                          final Serializer serializer) {
    this.blockHandler = blockHandler;
    this.ownershipCache = ownershipCache;
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
        blockSenderExecutor.submit(migration::startMigratingBlocks);
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

      // should run asynchronously to prevent deadlock in ownershipCache.updateOwnership()
      ownershipMsgHandlerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          // Update the owner of the block to the new one.
          // It waits until all operations release a read-lock on ownershipCache and acquires write-lock
          ownershipCache.updateOwnership(blockId, oldOwnerId, newOwnerId);

          sender.get().sendOwnershipAckMsg(Optional.of(senderId), operationId, blockId, oldOwnerId, newOwnerId,
                  traceInfo);
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

      // should run asynchronously to prevent deadlock in ownershipCache.updateOwnership()
      ownershipMsgHandlerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          // Update the owner of the block to the new one.
          // Operations being executed keep a read lock on ownershipCache while being executed.
          ownershipCache.updateOwnership(blockId, oldOwnerId, newOwnerId);

          // send block data to receiver
          ongoingMigrations.get(operationId).sendBlockData(blockId, traceInfo);

          // send ownership ack msg to driver
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

          // should allow access after putting a block
          blockHandler.putBlock(blockId, dataMap);
          ownershipCache.allowAccessToBlock(blockId);

          sender.get().sendDataAckMsg(senderId, blockId, operationId, traceInfo);
        }
      });
    } finally {
      Trace.continueSpan(detached).close();
    }
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
          blockHandler.removeBlock(blockId);

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
      this.blockIds = blockIds;

      this.parentTraceInfo = parentTraceInfo;
    }

    private void startMigratingBlocks() {
      int blockIdxToSend = blockIdxCounter.getAndIncrement();
      while (blockIdxToSend < blockIds.size()) {
        // can progress after acquiring a permit
        semaphore.acquireUninterruptibly();

        final int blockIdToMigrate = blockIds.get(blockIdxToSend);

        LOG.log(Level.FINE, "Start migrating a block. numTotalBlocksToSend: {0}, numSentBlocks: {1}," +
                " senderId: {2}, receiverId: {3}, blockId: {4}",
            new Object[]{blockIds.size(), blockIdxToSend, senderId, receiverId, blockIdToMigrate});

        final int oldOwnerId = getStoreId(senderId);
        final int newOwnerId = getStoreId(receiverId);

        try (TraceScope sendingBlockScope = Trace.startSpan(
            String.format("send_block. blockId: %d", blockIdToMigrate), parentTraceInfo)) {
          final TraceInfo traceInfo = TraceInfo.fromSpan(sendingBlockScope.getSpan());

          sender.get().sendOwnershipMsg(Optional.of(receiverId), senderId, operationId,
              blockIdToMigrate, oldOwnerId, newOwnerId, traceInfo);

          blockIdxToSend = blockIdxCounter.getAndIncrement();
        }
      }
    }

    private void sendBlockData(final int blockId, final TraceInfo traceInfo) {
      final Map<K, Object> blockData = blockHandler.getBlock(blockId);
      final List<KeyValuePair> keyValuePairs = toKeyValuePairs(blockData, serializer.getCodec());

      sender.get().sendDataMsg(receiverId, keyValuePairs, blockId, operationId, traceInfo);
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
    // and EMConfProvider.getServiceConfigurationWithoutNameResolver()).
    return Integer.valueOf(evalId.split("-")[1]);
  }
}
