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
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.exceptions.BlockAlreadyExistsException;
import edu.snu.cay.services.et.exceptions.BlockNotExistsException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An executor-side component that executes migration
 * directed by {@link edu.snu.cay.services.et.driver.impl.MigrationManager}.
 */
@Private
@EvaluatorSide
public final class MigrationExecutor implements EventHandler<MigrationMsg> {
  private static final Logger LOG = Logger.getLogger(MigrationExecutor.class.getName());

  private static final int MAX_CONCURRENT_MIGRATIONS = 4;
  private static final int NUM_BLOCK_SENDER_THREADS = 2;
  private static final int NUM_DATA_MSG_HANDLER_THREADS = 2;
  private static final int NUM_OWNERSHIP_MSG_HANDLER_THREADS = 2;

  // Thread pools to handle the messages in separate threads to prevent NCS threads' overhead.
  private final ExecutorService blockSenderExecutor = CatchableExecutors.newFixedThreadPool(NUM_BLOCK_SENDER_THREADS);
  private final ExecutorService dataMsgHandlerExecutor =
      CatchableExecutors.newFixedThreadPool(NUM_DATA_MSG_HANDLER_THREADS);
  private final ExecutorService ownershipMsgHandlerExecutor =
      CatchableExecutors.newFixedThreadPool(NUM_OWNERSHIP_MSG_HANDLER_THREADS);

  private final MessageSender msgSender;
  private final InjectionFuture<Tables> tablesFuture;

  /**
   * A map for maintaining ongoing migrations in sender-side.
   */
  private final Map<Long, Migration> ongoingMigrations = new ConcurrentHashMap<>();

  @Inject
  private MigrationExecutor(final MessageSender msgSender,
                            final InjectionFuture<Tables> tablesFuture) {
    this.msgSender = msgSender;
    this.tablesFuture = tablesFuture;
  }

  @Override
  public void onNext(final MigrationMsg msg) {
    switch (msg.getType()) {
    case MoveInitMsg:
      onMoveInitMsg(msg.getOperationId(), msg.getMoveInitMsg());
      break;

    case OwnershipMsg:
      onOwnershipMsg(msg.getOperationId(), msg.getOwnershipMsg());
      break;

    case OwnershipAckMsg:
      onOwnershipAckMsg(msg.getOperationId(), msg.getOwnershipAckMsg());
      break;

    case DataMsg:
      onDataMsg(msg.getOperationId(), msg.getDataMsg());
      break;

    case DataAckMsg:
      onDataAckMsg(msg.getOperationId(), msg.getDataAckMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private <K, V> void onMoveInitMsg(final long operationId, final MoveInitMsg msg) {
    final String senderId = msg.getSenderId();
    final String receiverId = msg.getReceiverId();
    final String tableId = msg.getTableId();
    final List<Integer> blockIds = msg.getBlockIds();

    LOG.log(Level.INFO, "OnMoveInitMsg. opId: {0}, tableId: {1}, oldOwnerId: {2}, newOwnerId: {3}," +
            " numBlocks: {4}, blockIds: {5}",
        new Object[]{operationId, tableId, senderId, receiverId, blockIds.size(), blockIds});

    try {
      final TableComponents<K, V, ?> tableComponents = tablesFuture.get().getTableComponents(tableId);

      final Migration<K, V> migration = new Migration<>(operationId, tableId, blockIds,
          senderId, receiverId, tableComponents);
      if (ongoingMigrations.put(operationId, migration) != null) {
        throw new RuntimeException("Migration already exist for id: " + operationId);
      }

      for (int i = 0; i < NUM_BLOCK_SENDER_THREADS; i++) {
        blockSenderExecutor.submit(migration::startMigratingBlocks);
      }
    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  private void onOwnershipMsg(final long operationId, final OwnershipMsg ownershipMsg) {
    final String tableId = ownershipMsg.getTableId();
    final int blockId = ownershipMsg.getBlockId();
    final String oldOwnerId = ownershipMsg.getOldOwnerId();
    final String newOwnerId = ownershipMsg.getNewOwnerId();

    LOG.log(Level.FINE, "OnOwnershipMsg. opId: {0}, tableId: {1}, blockId: {2}, oldOwnerId: {3}, newOwnerId: {4}",
        new Object[]{operationId, tableId, blockId, oldOwnerId, newOwnerId});

    try {
      final OwnershipCache ownershipCache = tablesFuture.get().getTableComponents(tableId).getOwnershipCache();

      // should run asynchronously to prevent deadlock in ownershipCache.updateOwnership()
      ownershipMsgHandlerExecutor.submit(() -> {
        // Update the owner of the block to the new one.
        // It waits until all operations release a read-lock on ownershipCache and acquires write-lock
        ownershipCache.update(blockId, oldOwnerId, newOwnerId);

        try {
          msgSender.sendOwnershipAckMsg(operationId, tableId, blockId, oldOwnerId, newOwnerId);
        } catch (NetworkException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  private void onOwnershipAckMsg(final long operationId, final OwnershipAckMsg ownershipAckMsg) {
    final int blockId = ownershipAckMsg.getBlockId();
    final String oldOwnerId = ownershipAckMsg.getOldOwnerId();
    final String newOwnerId = ownershipAckMsg.getNewOwnerId();
    final String tableId = ownershipAckMsg.getTableId();

    LOG.log(Level.FINE, "OnOwnershipAckMsg. opId: {0}, tableId: {1}, blockId: {2}, oldOwnerId: {3}, newOwnerId: {4}",
        new Object[]{operationId, tableId, blockId, oldOwnerId, newOwnerId});

    try {
      final OwnershipCache ownershipCache = tablesFuture.get().getTableComponents(tableId).getOwnershipCache();
      final Migration migration = ongoingMigrations.get(operationId);
      if (migration == null) {
        throw new RuntimeException("No ongoing migration for id: " + operationId);
      }

      // should run asynchronously to prevent deadlock in ownershipCache.updateOwnership()
      ownershipMsgHandlerExecutor.submit(() -> {
        // Update the owner of the block to the new one.
        // Operations being executed keep a read lock on ownershipCache while being executed.
        ownershipCache.update(blockId, oldOwnerId, newOwnerId);

        // send block data to receiver
        migration.sendDataMsg(blockId);

        // send ownershipMoved msg to driver
        try {
          msgSender.sendOwnershipMovedMsg(operationId, tableId, blockId);
        } catch (NetworkException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  private <K, V> void onDataMsg(final long operationId, final DataMsg dataMsg) {
    final String tableId = dataMsg.getTableId();
    final int blockId = dataMsg.getBlockId();
    final String senderId = dataMsg.getSenderId();
    final String receiverId = dataMsg.getReceiverId();

    LOG.log(Level.FINE, "OnDataMsg. opId: {0}, tableId: {1}, blockId: {2}, oldOwnerId: {3}, newOwnerId: {4}",
        new Object[]{operationId, tableId, blockId, senderId, receiverId});

    try {
      final TableComponents<K, V, ?> tableComponents = tablesFuture.get().getTableComponents(tableId);
      final BlockStore<K, V, ?> blockStore = tableComponents.getBlockStore();
      final OwnershipCache ownershipCache = tableComponents.getOwnershipCache();
      final KVUSerializer<K, V, ?> kvuSerializer = tableComponents.getSerializer();
      final boolean moveDataAndOwnershipTogether = !tableComponents.getTableMetadata().isMutableTable();

      dataMsgHandlerExecutor.submit(() -> {
        final Map<K, V> dataMap = toDataMap(dataMsg.getKvPairs(), kvuSerializer);

        // should allow access after putting a block
        try {
          blockStore.putBlock(blockId, dataMap);

          if (moveDataAndOwnershipTogether) { // for immutable tables
            ownershipCache.update(blockId, senderId, receiverId);
          }
          ownershipCache.allowAccessToBlock(blockId);
          msgSender.sendDataAckMsg(operationId, tableId, blockId, senderId, receiverId);
        } catch (final BlockAlreadyExistsException | NetworkException e) {
          throw new RuntimeException(e);
        }
      });

    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  private void onDataAckMsg(final long operationId, final DataAckMsg dataAckMsg) {
    final String tableId = dataAckMsg.getTableId();
    final int blockId = dataAckMsg.getBlockId();
    final String senderId = dataAckMsg.getSenderId();
    final String receiverId = dataAckMsg.getReceiverId();

    LOG.log(Level.FINE, "OnDataAckMsg. opId: {0}, tableId: {1}, blockId: {2}, oldOwnerId: {3}, newOwnerId: {4}",
        new Object[]{operationId, tableId, blockId, senderId, receiverId});

    final Migration migration = ongoingMigrations.get(operationId);
    if (migration == null) {
      throw new RuntimeException("No ongoing migration for id: " + operationId);
    }

    if (migration.finishMigratingBlock()) {
      ongoingMigrations.remove(operationId);
    }

    try {
      final TableComponents tableComponents = tablesFuture.get().getTableComponents(tableId);
      final OwnershipCache ownershipCache = tableComponents.getOwnershipCache();
      final BlockStore blockStore = tableComponents.getBlockStore();
      final boolean moveDataAndOwnershipTogether = !tableComponents.getTableMetadata().isMutableTable();

      dataMsgHandlerExecutor.submit(() -> {
        // After the data is migrated, it's safe to remove the local data block.
        try {
          if (moveDataAndOwnershipTogether) { // for immutable tables
            ownershipCache.update(blockId, senderId, receiverId);
          }
          blockStore.removeBlock(blockId);
          msgSender.sendDataMovedMsg(operationId, tableId, blockId, moveDataAndOwnershipTogether);
        } catch (final BlockNotExistsException | NetworkException e) {
          throw new RuntimeException(e);
        }
      });

    } catch (final TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Abstraction of a single migration for managing states
   * while sending Ownership and Data msgs and receiving corresponding ack msgs.
   */
  private final class Migration<K, V> {
    // metadata of migration
    private final long operationId;

    private final String tableId;
    private final String senderId;
    private final String receiverId;
    private final List<Integer> blockIds;

    private final boolean moveDataAndOwnershipTogether;
    private final BlockStore<K, V, ?> blockStore;
    private final KVUSerializer<K, V, ?> kvuSerializer;

    // state of migration
    private final AtomicInteger blockIdxCounter = new AtomicInteger(0);
    private final AtomicInteger migratedBlockCounter = new AtomicInteger(0);
    private final AtomicInteger numSentKVEntries = new AtomicInteger(0);
    private final AtomicInteger numSentKeyBytes = new AtomicInteger(0);
    private final AtomicInteger numSentValueBytes = new AtomicInteger(0);

    // semaphore to restrict the number of concurrent block migration
    private final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_MIGRATIONS);

    Migration(final long operationId, final String tableId, final List<Integer> blockIds,
              final String senderId, final String receiverId, final TableComponents<K, V, ?> tableComponents) {
      this.operationId = operationId;
      this.tableId = tableId;
      this.blockIds = blockIds;
      this.senderId = senderId;
      this.receiverId = receiverId;
      this.moveDataAndOwnershipTogether = !tableComponents.getTableMetadata().isMutableTable();
      this.blockStore = tableComponents.getBlockStore();
      this.kvuSerializer = tableComponents.getSerializer();
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

        if (moveDataAndOwnershipTogether) {
          sendDataMsg(blockIdToMigrate);
        } else {
          sendOwnershipMsg(blockIdToMigrate);
        }

        blockIdxToSend = blockIdxCounter.getAndIncrement();
      }
    }

    private void sendOwnershipMsg(final int blockId) {
      try {
        msgSender.sendOwnershipMsg(operationId, tableId, blockId, senderId, receiverId);
      } catch (NetworkException e) {
        throw new RuntimeException(e);
      }
    }

    private void sendDataMsg(final int blockId) {
      try {
        final Map<K, V> blockData = blockStore.getBlock(blockId);
        final List<KVPair> keyValuePairs = toKeyValuePairs(blockData, kvuSerializer);
        numSentKVEntries.getAndAdd(keyValuePairs.size());
        keyValuePairs.forEach(kvPair -> {
          numSentKeyBytes.getAndAdd(kvPair.getKey().getKey().array().length);
          numSentValueBytes.getAndAdd(kvPair.getValue().getValue().array().length);
        });
        msgSender.sendDataMsg(operationId, tableId, blockId, keyValuePairs, senderId, receiverId);
      } catch (final BlockNotExistsException | NetworkException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Finish migration of a single block and let one thread to start migrating a next block.
     * @return True, if all blocks are migrated
     */
    private boolean finishMigratingBlock() {
      semaphore.release();

      final boolean allBlocksMoved = migratedBlockCounter.incrementAndGet() == blockIds.size();

      if (allBlocksMoved) {
        LOG.log(Level.INFO, "OpId: {0}, numSentBlocks: {1}, numSentKVEntries: {2}," +
                " numSentKeyBytes: {3}, numSentValueBytes: {4}", new Object[]{operationId,
            blockIds.size(), numSentKVEntries.get(), numSentKeyBytes.get(), numSentValueBytes.get()});
      }

      return allBlocksMoved;
    }
  }

  private <K, V> List<KVPair> toKeyValuePairs(final Map<K, V> blockData,
                                              final KVUSerializer<K, V, ?> kvuSerializer) {
    final List<KVPair> kvPairs = new ArrayList<>(blockData.size());
    for (final Map.Entry<K, V> entry : blockData.entrySet()) {
      final DataKey dataKey = DataKey.newBuilder()
          .setKey(ByteBuffer.wrap(kvuSerializer.getKeyCodec().encode(entry.getKey())))
          .build();
      final DataValue dataValue = DataValue.newBuilder()
          .setValue(ByteBuffer.wrap(kvuSerializer.getValueCodec().encode(entry.getValue())))
          .build();

      kvPairs.add(KVPair.newBuilder()
          .setKey(dataKey)
          .setValue(dataValue)
          .build());
    }
    return kvPairs;
  }

  private <K, V> Map<K, V> toDataMap(final List<KVPair> kvPairs,
                                     final KVUSerializer<K, V, ?> kvuSerializer) {
    final Map<K, V> dataMap = new HashMap<>(kvPairs.size());
    for (final KVPair kvPair : kvPairs) {
      final DataKey dataKey = kvPair.getKey();
      final DataValue dataValue = kvPair.getValue();

      dataMap.put(kvuSerializer.getKeyCodec().decode(dataKey.getKey().array()),
          kvuSerializer.getValueCodec().decode(dataValue.getValue().array()));
    }
    return dataMap;
  }
}
