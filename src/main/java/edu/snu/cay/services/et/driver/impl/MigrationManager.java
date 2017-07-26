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

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.common.impl.CallbackRegistry;
import edu.snu.cay.services.et.common.util.concurrent.ResultFuture;
import edu.snu.cay.services.et.driver.api.MessageSender;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
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
  private final SubscriptionManager subscriptionManager;
  private final CallbackRegistry callbackRegistry;

  private final AtomicLong opIdCounter = new AtomicLong(0);

  /**
   * This is a mapping from operation id to the {@link Migration}
   * which consists of the current state of each migration.
   */
  private final Map<Long, Migration> ongoingMigrations = new ConcurrentHashMap<>();

  @Inject
  private MigrationManager(final MessageSender msgSender,
                           final SubscriptionManager subscriptionManager,
                           final CallbackRegistry callbackRegistry) {
    this.msgSender = msgSender;
    this.subscriptionManager = subscriptionManager;
    this.callbackRegistry = callbackRegistry;
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
   */
  synchronized ListenableFuture<MigrationResult> startMigration(final BlockManager blockManager,
                                                                final String tableId,
                                                                final String srcExecutorId, final String dstExecutorId,
                                                                final List<Integer> blockIds) {
    final long opId = opIdCounter.getAndIncrement();
    ongoingMigrations.put(opId, new Migration(srcExecutorId, dstExecutorId, tableId, blockIds, blockManager));

    final ResultFuture<MigrationResult> resultFuture = new ResultFuture<>();
    resultFuture.addListener(LOGGING_CALLBACK);

    final EventHandler<MigrationResult> callbackToRegister = resultFuture::onCompleted;

    callbackRegistry.register(MigrationResult.class, Long.toString(opId), callbackToRegister);

    msgSender.sendMoveInitMsg(opId, tableId, blockIds, srcExecutorId, dstExecutorId);
    return resultFuture;
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

    updateBlockOwnership(migration, blockId);

    LOG.log(Level.FINE, "Ownership moved. opId: {0}, blockId: {1}.", new Object[]{opId, blockId});
  }

  private synchronized void updateBlockOwnership(final Migration migration, final int blockId) {
    final Migration.MigrationMetadata migrationMetadata = migration.getMigrationMetadata();
    final String tableId = migrationMetadata.getTableId();
    final String senderId = migrationMetadata.getSenderId();
    final String receiverId = migrationMetadata.getReceiverId();

    migration.getBlockManager().updateOwner(blockId, senderId, receiverId);
    subscriptionManager.broadcastUpdate(tableId, blockId, senderId, receiverId);
  }

  /**
   * Marks a block as moved and finishes a migration if all blocks of the migration have been migrated.
   * After this call, the block can be chosen for other migrations.
   * @param opId Identifier of {@code move} operation.
   * @param blockId Identifier of the moved block.
   */
  synchronized void dataMoved(final long opId, final int blockId) {
    final Migration migration = ongoingMigrations.get(opId);
    if (migration == null) {
      throw new RuntimeException(String.format("Migration with ID %d was not registered," +
          " or it has already been finished.", opId));
    }

    LOG.log(Level.FINE, "Data moved. opId: {0}, blockId: {1}", new Object[]{opId, blockId});

    blockMigrationCompleted(migration, opId, blockId);
  }

  private synchronized void blockMigrationCompleted(final Migration migration, final long opId, final int blockId) {
    migration.markBlockAsMoved(blockId);
    migration.getBlockManager().releaseBlockFromMove(blockId);
    LOG.log(Level.INFO, "Block migration has been completed. opId: {0}, tableId: {1}, blockId: {2}",
        new Object[]{opId, migration.getMigrationMetadata().getTableId(), blockId});

    if (migration.isComplete()) {
      ongoingMigrations.remove(opId);
      final String msg = String.format("%d blocks have been successfully migrated",
          migration.getMigrationMetadata().getBlockIds().size());
      callbackRegistry.onCompleted(MigrationResult.class, Long.toString(opId),
          new MigrationResult(true, msg, migration.getMovedBlocks()));
    }
  }

  /**
   * Do all things in each {@link #ownershipMoved} and {@link #dataMoved}.
   * @param opId an operation id
   * @param blockId a block id
   */
  synchronized void dataAndOwnershipMoved(final long opId, final int blockId) {
    final Migration migration = ongoingMigrations.get(opId);
    if (migration == null) {
      throw new RuntimeException(String.format("Migration with ID %d was not registered," +
          " or it has already been finished.", opId));
    }

    updateBlockOwnership(migration, blockId);
    blockMigrationCompleted(migration, opId, blockId);

    LOG.log(Level.FINE, "Data and ownership moved. opId: {0}, blockId: {1}.", new Object[]{opId, blockId});
  }
}
