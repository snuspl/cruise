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
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
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
 */
public final class OwnershipFirstMigrationExecutor<K> implements MigrationExecutor {
  private static final Logger LOG = Logger.getLogger(OwnershipFirstMigrationExecutor.class.getName());

  // limit the number of concurrent migrations to prevent too much gap between ownership transition and data transfer
  private static final int MAX_CONCURRENT_MIGRATIONS = 4;
  private static final int NUM_MIGRATION_THREADS = 2;

  private final RemoteAccessibleMemoryStore<K> memoryStore;
  private final OperationRouter router;
  private final InjectionFuture<ElasticMemoryMsgSender> sender;

  private final ExecutorService blockMigrationExecutor = Executors.newFixedThreadPool(NUM_MIGRATION_THREADS);

  private final Map<String, Migration> ongoingMigrations = new ConcurrentHashMap<>();

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

    case DataMsg:
      onDataMsg(msg);
      break;

    case DataAckMsg:
      onDataAckMsg(msg);
      break;

    case OwnershipMsg:
      onOwnershipMsg(msg);
      break;

    case OwnershipAckMsg:
      onOwnershipAckMsg(msg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onMoveInitMsg(final MigrationMsg msg) {
    Trace.setProcessId("src_eval");
    try (final TraceScope onMoveInitMsgScope = Trace.startSpan("on_move_init_msg",
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final String operationId = msg.getOperationId().toString();

      final MoveInitMsg moveInitMsg = msg.getMoveInitMsg();
      final String senderId = moveInitMsg.getSenderId().toString();
      final String receiverId = moveInitMsg.getReceiverId().toString();
      final List<Integer> blockIds = moveInitMsg.getBlockIds();

      final TraceInfo traceInfo = TraceInfo.fromSpan(onMoveInitMsgScope.getSpan());

      final Migration migration = new Migration(operationId, senderId, receiverId, blockIds, traceInfo);
      ongoingMigrations.put(operationId, migration);

      for (int i = 0; i < NUM_MIGRATION_THREADS; i++) {
        blockMigrationExecutor.submit(new BlockMigratingThread(migration));
      }
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
    private final TraceInfo traceInfo;

    // state of migration
    private final AtomicInteger blockIdxCounter = new AtomicInteger(0);
    private final AtomicInteger migratedBlockCounter = new AtomicInteger(0);
    private final BlockingQueue<Token> tokenBlockingQueue = new ArrayBlockingQueue<>(MAX_CONCURRENT_MIGRATIONS);

    Migration(final String operationId, final String senderId, final String receiverId, final List<Integer> blockIds,
              final TraceInfo traceInfo) {
      this.operationId = operationId;
      this.senderId = senderId;
      this.receiverId = receiverId;
      this.blockIds = Collections.unmodifiableList(blockIds);
      this.traceInfo = traceInfo;

      for (int i = 0; i < MAX_CONCURRENT_MIGRATIONS; i++) {
        tokenBlockingQueue.add(new Token());
      }
    }

    // empty class for token abstraction that gives chance of sending a block.
    private final class Token {

    }

    private void startMigratingBlock() {
      int blockIdxToSend = blockIdxCounter.getAndIncrement();

      while (blockIdxToSend < blockIds.size()) {
        final int blockIdToMigrate = blockIds.get(blockIdxToSend);

        LOG.log(Level.INFO, "start migrating a block. blockIdxToSend: {0} senderId: {1}, receiverId: {2}, blockId: {3}",
            new Object[]{blockIdxToSend, senderId, receiverId, blockIdToMigrate});

        // can progress after obtaining token
        try {
          tokenBlockingQueue.take();
        } catch (final InterruptedException e) {
          throw new RuntimeException("Interrupted while waiting for tokens", e);
        }

        // block clients's access before starting migration
        router.markBlockAsMigrating(blockIdToMigrate);

        final Map<K, Object> blockData = memoryStore.getBlock(blockIdToMigrate);
        final List<KeyValuePair> keyValuePairs = toKeyValuePairs(blockData, serializer.getCodec());

        final int oldOwnerId = getStoreId(senderId);
        final int newOwnerId = getStoreId(receiverId);

        // send ownership msg and data msg at once
        sender.get().sendOwnershipMsg(Optional.of(receiverId), senderId, operationId,
            blockIdToMigrate, oldOwnerId, newOwnerId, null);

        sender.get().sendDataMsg(receiverId, keyValuePairs, blockIdToMigrate, operationId, null);

        blockIdxToSend = blockIdxCounter.getAndIncrement();
      }
    }

    private boolean finishMigratingBlock() {
      tokenBlockingQueue.add(new Token());
      return migratedBlockCounter.incrementAndGet() == blockIds.size();
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
  }

  private final class BlockMigratingThread implements Runnable {
    private final Migration migration;

    BlockMigratingThread(final Migration migration) {
      this.migration = migration;
    }

    @Override
    public void run() {
      migration.startMigratingBlock();
    }
  }

  private final Map<Integer, IncomingBlock> incomingBlocks = Collections.synchronizedMap(new HashMap<>());

  private final class IncomingBlock {
    private Map<K, Object> dataMap;

    IncomingBlock() {
    }

    IncomingBlock(final Map<K, Object> dataMap) {
      this.dataMap = dataMap;
    }
  }

  private void onDataMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final DataMsg dataMsg = msg.getDataMsg();
    final String senderId = dataMsg.getSenderId().toString();
    final int blockId = dataMsg.getBlockId();

    Trace.setProcessId("dst_eval");
    try (final TraceScope onDataMsgScope = Trace.startSpan(String.format("on_data_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final TraceInfo traceInfo = TraceInfo.fromSpan(onDataMsgScope.getSpan());

      final Map<K, Object> dataMap;
      try (final TraceScope decodeDataScope = Trace.startSpan("decode_data", traceInfo)) {
        dataMap = toDataMap(dataMsg.getKeyValuePairs(), serializer.getCodec());
      }

      final boolean ownershipMsgArrivedFirst;
      synchronized (incomingBlocks) {
        if (!incomingBlocks.containsKey(blockId)) {
          ownershipMsgArrivedFirst = false;
          incomingBlocks.put(blockId, new IncomingBlock(dataMap));
        } else {
          ownershipMsgArrivedFirst = true;
          incomingBlocks.remove(blockId);
        }
      }

      if (ownershipMsgArrivedFirst) {
        handleDataMsg(operationId, senderId, blockId, dataMap, traceInfo);
      }
    }
  }

  private void handleDataMsg(final String operationId, final String senderId,
                             final int blockId, final Map<K, Object> dataMap, final TraceInfo traceInfo) {
    memoryStore.putBlock(blockId, dataMap);

    // wake up waiting client threads to access immigrated data
    router.unMarkBlockFromMigrating(blockId);

    sender.get().sendDataAckMsg(senderId, blockId, operationId, traceInfo);
  }

  private void onDataAckMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final DataAckMsg dataAckMsg = msg.getDataAckMsg();
    final int blockId = dataAckMsg.getBlockId();

    Trace.setProcessId("src_eval");
    try (final TraceScope onOwnershipMsgScope = Trace.startSpan(String.format("on_data_ack_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final Migration migration = ongoingMigrations.get(operationId);
      if (migration.finishMigratingBlock()) {
        ongoingMigrations.remove(operationId, migration);
      }

      // After the data is migrated, it's safe to remove the local data block.
      memoryStore.removeBlock(blockId);

      sender.get().sendBlockMovedMsg(operationId, blockId, TraceInfo.fromSpan(onOwnershipMsgScope.getSpan()));
    }
  }

  private final ExecutorService ownershipUpdateExecutor = Executors.newFixedThreadPool(2);

  private final AtomicInteger numReceivedOwnershipMsg = new AtomicInteger(0); // for debug

  private void onOwnershipMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final OwnershipMsg ownershipMsg = msg.getOwnershipMsg();
    final int blockId = ownershipMsg.getBlockId();
    final String senderId = ownershipMsg.getSenderId().toString();
    final int oldOwnerId = ownershipMsg.getOldOwnerId();
    final int newOwnerId = ownershipMsg.getNewOwnerId();

    Trace.setProcessId("dest_eval");
    try (final TraceScope onOwnershipMsgScope = Trace.startSpan(String.format("on_ownership_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      ownershipUpdateExecutor.submit(new Runnable() {
        @Override
        public void run() {
          // clients should wait for DataMsg
          router.markBlockAsMigrating(blockId);

          final boolean ownershipMsgArrivedFirst;
          synchronized (incomingBlocks) {
            if (!incomingBlocks.containsKey(blockId)) {
              ownershipMsgArrivedFirst = true;
              incomingBlocks.put(blockId, new IncomingBlock());
            } else {
              ownershipMsgArrivedFirst = false;
            }

            // Update the owner of the block to the new one.
            // Operations being executed keep a read lock on router while being executed.
            router.updateOwnership(blockId, oldOwnerId, newOwnerId);
          }

          final TraceInfo traceInfo = TraceInfo.fromSpan(onOwnershipMsgScope.getSpan());

          sender.get().sendOwnershipAckMsg(Optional.of(senderId), operationId, blockId, oldOwnerId, newOwnerId,
                  traceInfo);

          if (!ownershipMsgArrivedFirst) {
            final IncomingBlock block = incomingBlocks.remove(blockId);
            handleDataMsg(operationId, senderId, blockId, block.dataMap, traceInfo);
          }
        }
      });
    }
  }

  private void onOwnershipAckMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final OwnershipAckMsg ownershipAckMsg = msg.getOwnershipAckMsg();
    final int blockId = ownershipAckMsg.getBlockId();
    final int oldOwnerId = ownershipAckMsg.getOldOwnerId();
    final int newOwnerId = ownershipAckMsg.getNewOwnerId();

    Trace.setProcessId("src_eval");
    try (final TraceScope onOwnershipAckMsgScope = Trace.startSpan(
        String.format("on_ownership_ack_msg. blockId: %d", blockId), HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      ownershipUpdateExecutor.submit(new Runnable() {
        @Override
        public void run() {
          // Update the owner of the block to the new one.
          // Operations being executed keep a read lock on router while being executed.
          router.updateOwnership(blockId, oldOwnerId, newOwnerId);

          // wake up blocking client threads to access emigrated data
          router.unMarkBlockFromMigrating(blockId);

          sender.get().sendOwnershipAckMsg(Optional.empty(), operationId, blockId, oldOwnerId, newOwnerId,
              TraceInfo.fromSpan(onOwnershipAckMsgScope.getSpan()));
        }
      });
    }
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
