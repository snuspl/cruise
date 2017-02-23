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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.common.impl.CallbackRegistry;
import edu.snu.cay.services.et.driver.api.MessageSender;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A manager class that administrates migration of blocks between executors.
 * Note that tables share one instance of {@link MigrationManager}.
 */
@Private
@DriverSide
final class MigrationManager {
  private static final Logger LOG = Logger.getLogger(MigrationManager.class.getName());

  private static final EventHandler<MigrationResult> LOGGING_CALLBACK =
      migrationResult -> LOG.log(Level.INFO, migrationResult.getMsg());

  private final MessageSender msgSender;
  private final CallbackRegistry callbackRegistry;

  private final AtomicLong opIdCounter = new AtomicLong(0);

  /**
   * This is a mapping from operation id to the {@link Migration}
   * which consists of the current state of each migration.
   */
  private final Map<Long, Migration> ongoingMigrations = new ConcurrentHashMap<>();

  /**
   * A mapping between table id and a set of ids of corresponding subscribers.
   */
  private final Map<String, Set<String>> subscribersPerTable = new ConcurrentHashMap<>();

  @Inject
  private MigrationManager(final MessageSender msgSender,
                           final CallbackRegistry callbackRegistry) {
    this.msgSender = msgSender;
    this.callbackRegistry = callbackRegistry;
  }

  /**
   * Registers subscribers for the update of partition status of a table whose id is {@code tableId}.
   * Whenever a block has been moved, the executor with {@code executorId} will be notified.
   * @param tableId a table id
   * @param executorId a executor id
   */
  void registerSubscription(final String tableId, final String executorId) {
    subscribersPerTable.compute(tableId, (tId, executorIdSet) -> {
      final Set<String> value = executorIdSet == null ? Collections.synchronizedSet(new HashSet<>()) : executorIdSet;
      if (!value.add(executorId)) {
        throw new RuntimeException(String.format("Table %s already has subscriber %s", tId, executorId));
      }

      return value;
    });
  }

  /**
   * Registers subscribers for the update of partition status of a table whose id is {@code tableId}.
   * Whenever a block has been moved, the executor with {@code executorId} will be notified.
   * @param tableId a table id
   * @param executorId a executor id
   */
  void unregisterSubscription(final String tableId, final String executorId) {
    subscribersPerTable.compute(tableId, (tId, executorIdSet) -> {
      if (executorIdSet == null) {
        throw new RuntimeException(String.format("Table %s does not exist", tId));
      }
      if (!executorIdSet.remove(executorId)) {
        throw new RuntimeException(String.format("Table %s does not have subscriber %s", tId, executorId));
      }

      return executorIdSet.isEmpty() ? null : executorIdSet;
    });
  }

  /**
   * Starts migration of the blocks with ids of {@code blockIds} within table {@code tableId}
   * from src executor to dst executor. The state of {@link BlockManager} will be updated this by this migration.
   * When the migration is finished callback will be invoked with {@link MigrationResult}.
   * @param blockManager a {@link BlockManager} of a table to be moved
   * @param tableId a table id
   * @param srcExecutorId an id of src executor
   * @param dstExecutorId an id of dst executor
   * @param blockIds a list of block ids to move
   * @param callback a callback that will be called upon the finish of migration
   */
  synchronized void startMigration(final BlockManager blockManager,
                                   final String tableId,
                                   final String srcExecutorId, final String dstExecutorId,
                                   final List<Integer> blockIds,
                                   @Nullable final EventHandler<MigrationResult> callback) {
    final long opId = opIdCounter.getAndIncrement();
    ongoingMigrations.put(opId, new Migration(srcExecutorId, dstExecutorId, tableId, blockIds, blockManager));

    final EventHandler<MigrationResult> callbackToRegister = callback != null ? callback : LOGGING_CALLBACK;
    callbackRegistry.register(MigrationResult.class, Long.toString(opId), callbackToRegister);

    msgSender.sendMoveInitMsg(opId, tableId, blockIds, srcExecutorId, dstExecutorId);
  }

  /**
   * Updates the owner of the block and broadcast it to subscribers.
   * It only sends messages to subscribers excluding the source and destination of the migration,
   * because their ownership caches are already updated during the migration.
   * @param opId Identifier of {@code move} operation.
   * @param blockId id of the block
   */
  synchronized void ownershipMoved(final long opId, final int blockId) {
    final Migration migration = ongoingMigrations.get(opId);
    if (migration == null) {
      throw new RuntimeException(String.format("Migration with ID %d was not registered," +
          " or it has already been finished.", opId));
    }

    final Migration.MigrationMetadata migrationMetadata = migration.getMigrationMetadata();
    final String tableId = migrationMetadata.getTableId();
    final String senderId = migrationMetadata.getSenderId();
    final String receiverId = migrationMetadata.getReceiverId();

    migration.getBlockManager().updateOwner(blockId, senderId, receiverId);

    final Set<String> subscribers = new HashSet<>(subscribersPerTable.get(tableId));
    subscribers.remove(senderId);
    subscribers.remove(receiverId);

    LOG.log(Level.FINE, "Ownership moved. opId: {0}, blockId: {1}." +
        " Broadcast the ownership update to other subscribers: {2}", new Object[]{opId, blockId, subscribers});

    subscribers.forEach(executorId ->
        msgSender.sendOwnershipUpdateMsg(executorId, tableId, blockId, senderId, receiverId));
  }

  /**
   * Marks a block as moved and finishes a migration if all blocks of the migration have been migrated.
   * After this call, the block can be chosen for other migrations.
   * @param opId Identifier of {@code move} operation.
   * @param blockId Identifier of the moved block.
   */
  synchronized void markBlockAsMoved(final long opId, final int blockId) {
    final Migration migration = ongoingMigrations.get(opId);
    if (migration == null) {
      throw new RuntimeException(String.format("Migration with ID %d was not registered," +
          " or it has already been finished.", opId));
    }

    migration.markBlockAsMoved(blockId);
    migration.getBlockManager().releaseBlockFromMove(blockId);

    LOG.log(Level.FINE, "Data moved. opId: {0}, blockId: {1}", new Object[]{opId, blockId});

    if (migration.isComplete()) {
      ongoingMigrations.remove(opId);
      final String msg = String.format("%d blocks have been successfully migrated",
          migration.getMigrationMetadata().getBlockIds().size());
      callbackRegistry.onCompleted(MigrationResult.class, Long.toString(opId),
          new MigrationResult(true, msg, migration.getMovedBlocks()));
    }
  }
}
